/*****************************************************************************

Copyright (c) 1996, 2016, Oracle and/or its affiliates. All Rights Reserved.
Copyright (c) 2014, 2021, MariaDB Corporation.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License as published by the Free Software
Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA

*****************************************************************************/

/**************************************************//**
@file lock/lock0wait.cc
The transaction lock system

Created 25/5/2010 Sunny Bains
*******************************************************/

#define LOCK_MODULE_IMPLEMENTATION

#include "univ.i"
#include <mysql/service_thd_wait.h>
#include <mysql/service_wsrep.h>

#include "srv0mon.h"
#include "que0que.h"
#include "lock0lock.h"
#include "row0mysql.h"
#include "srv0start.h"
#include "lock0priv.h"

#ifdef WITH_WSREP
/*********************************************************************//**
check if lock timeout was for priority thread,
as a side effect trigger lock monitor
@param[in]    trx    transaction owning the lock
@param[in]    locked true if trx and lock_sys.mutex is ownd
@return	false for regular lock timeout */
static
bool
wsrep_is_BF_lock_timeout(
	const trx_t*	trx,
	bool		locked = true)
{
	if (trx->error_state != DB_DEADLOCK && trx->is_wsrep() &&
	    srv_monitor_timer && wsrep_thd_is_BF(trx->mysql_thd, FALSE)) {
		ib::info() << "WSREP: BF lock wait long for trx:" << ib::hex(trx->id)
			   << " query: " << wsrep_thd_query(trx->mysql_thd);
		if (!locked) {
			lock_sys.mutex_lock();
		}

		lock_sys.mutex_assert_locked();

		trx_print_latched(stderr, trx, 3000);

		if (!locked) {
			lock_sys.mutex_unlock();
		}

		srv_print_innodb_monitor 	= TRUE;
		srv_print_innodb_lock_monitor 	= TRUE;
		srv_monitor_timer_schedule_now();
		return true;
	}
	return false;
}
#endif /* WITH_WSREP */

/** Wait for a lock to be released.
@retval DB_DEADLOCK if this transaction was chosen as the deadlock victim
@retval DB_INTERRUPTED if the execution was interrupted by the user
@retval DB_LOCK_WAIT_TIMEOUT if the lock wait timed out
@retval DB_SUCCESS if the lock was granted */
dberr_t lock_wait(que_thr_t *thr)
{
  trx_t *trx= thr_get_trx(thr);

  if (trx->mysql_thd)
    DEBUG_SYNC_C("lock_wait_suspend_thread_enter");

  /* InnoDB system transactions may use the global value of
  innodb_lock_wait_timeout, because trx->mysql_thd == NULL. */
  const ulong innodb_lock_wait_timeout= trx_lock_wait_timeout_get(trx);
  const bool no_timeout= innodb_lock_wait_timeout > 100000000;
  const my_hrtime_t suspend_time= my_hrtime_coarse();
  ut_ad(!trx->dict_operation_lock_mode ||
        trx->dict_operation_lock_mode == RW_S_LATCH);
  const bool row_lock_wait= thr->lock_state == QUE_THR_LOCK_ROW;
  bool had_dict_lock= trx->dict_operation_lock_mode != 0;

  mysql_mutex_lock(&lock_sys.wait_mutex);
  trx->mutex.wr_lock();
  trx->error_state= DB_SUCCESS;

  ut_ad(thr->is_active == (thr->state == QUE_THR_RUNNING));

  if (thr->is_active)
  {
    /* The lock has already been released or this transaction
    was chosen as a deadlock victim: no need to suspend */

    if (trx->lock.was_chosen_as_deadlock_victim)
    {
      trx->error_state= DB_DEADLOCK;
      trx->lock.was_chosen_as_deadlock_victim= false;
    }

    mysql_mutex_unlock(&lock_sys.wait_mutex);
    trx->mutex.wr_unlock();
    return trx->error_state;
  }

  trx->lock.suspend_time= suspend_time;
  trx->mutex.wr_unlock();

  if (row_lock_wait)
  {
    // FIXME: use normal counters protected by lock_sys.wait_mutex
    srv_stats.n_lock_wait_count.inc();
    srv_stats.n_lock_wait_current_count++;
  }

  int err= 0;

  /* The wait_lock can be cleared by another thread in lock_grant(),
  lock_rec_cancel(), or lock_cancel_waiting_and_release(). But, a wait
  can only be initiated by the current thread which owns the transaction. */
  if (const lock_t *wait_lock= trx->lock.wait_lock)
  {
    if (had_dict_lock) /* Release foreign key check latch */
    {
      mysql_mutex_unlock(&lock_sys.wait_mutex);
      row_mysql_unfreeze_data_dictionary(trx);
      mysql_mutex_lock(&lock_sys.wait_mutex);
    }
    timespec abstime;
    set_timespec_time_nsec(abstime, suspend_time.val * 1000);
    abstime.MY_tv_sec+= innodb_lock_wait_timeout;
    thd_wait_begin(trx->mysql_thd, lock_get_type_low(wait_lock) == LOCK_TABLE
                   ? THD_WAIT_TABLE_LOCK : THD_WAIT_ROW_LOCK);
    while (trx->lock.wait_lock)
    {
      if (no_timeout)
        mysql_cond_wait(&trx->lock.cond, &lock_sys.wait_mutex);
      else
        err= mysql_cond_timedwait(&trx->lock.cond, &lock_sys.wait_mutex,
                                  &abstime);
      switch (trx->error_state) {
      default:
        if (trx_is_interrupted(trx))
          trx->error_state= DB_INTERRUPTED;
        else if (!err)
          continue;
        else
          break;
        /* fall through */
      case DB_DEADLOCK:
      case DB_INTERRUPTED:
        err= 0;
      }
      break;
    }
    thd_wait_end(trx->mysql_thd);
  }
  else
    had_dict_lock= false;

  if (row_lock_wait)
    srv_stats.n_lock_wait_current_count--;

  if (row_lock_wait)
  {
    const my_hrtime_t resume_time= my_hrtime_coarse();
    if (resume_time.val >= suspend_time.val)
    {
      const ulint diff_time= static_cast<ulint>
        ((resume_time.val - suspend_time.val) / 1000);
      srv_stats.n_lock_wait_time.add(diff_time); // FIXME: use normal variable

      if (diff_time > lock_sys.n_lock_max_wait_time)
        lock_sys.n_lock_max_wait_time= diff_time;

      thd_storage_lock_wait(trx->mysql_thd, diff_time);
    }
  }

  mysql_mutex_unlock(&lock_sys.wait_mutex);

  if (had_dict_lock)
    row_mysql_freeze_data_dictionary(trx);

  if (!err);
#ifdef WITH_WSREP
  else if (trx->is_wsrep() && wsrep_is_BF_lock_timeout(trx, false));
#endif
  else
  {
    trx->error_state= DB_LOCK_WAIT_TIMEOUT;
    MONITOR_INC(MONITOR_TIMEOUT);
  }

  if (trx->lock.wait_lock)
  {
    {
      LockMutexGuard g;
      mysql_mutex_lock(&lock_sys.wait_mutex);
      if (lock_t *lock= trx->lock.wait_lock)
      {
        trx->mutex.wr_lock();
        lock_cancel_waiting_and_release(lock);
        trx->mutex.wr_unlock();
      }
      mysql_mutex_unlock(&lock_sys.wait_mutex);
    }
  }

  return trx->error_state;
}

/********************************************************************//**
Releases a user OS thread waiting for a lock to be released, if the
thread is already suspended. */
void
lock_wait_release_thread_if_suspended(
/*==================================*/
	que_thr_t*	thr)	/*!< in: query thread associated with the
				user OS thread	 */
{
  lock_sys.mutex_assert_locked();
  mysql_mutex_assert_owner(&lock_sys.wait_mutex);
  trx_t *trx= thr_get_trx(thr);
  if (trx->lock.was_chosen_as_deadlock_victim)
  {
    trx->error_state= DB_DEADLOCK;
    trx->lock.was_chosen_as_deadlock_victim = false;
  }
  mysql_cond_signal(&trx->lock.cond);
}
