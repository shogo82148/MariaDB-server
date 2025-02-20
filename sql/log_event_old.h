/* Copyright (c) 2007, 2013, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1335  USA */

#ifndef LOG_EVENT_OLD_H
#define LOG_EVENT_OLD_H

/*
  Need to include this file at the proper position of log_event.h
 */


/**
  @file

  @brief This file contains classes handling old formats of row-based
  binlog events.
*/
/*
  Around 2007-10-31, I made these classes completely separated from
  the new classes (before, there was a complex class hierarchy
  involving multiple inheritance; see BUG#31581), by simply copying
  and pasting the entire contents of Rows_log_event into
  Old_rows_log_event and the entire contents of
  {Write|Update|Delete}_rows_log_event into
  {Write|Update|Delete}_rows_log_event_old.  For clarity, I will keep
  the comments marking which code was cut-and-pasted for some time.
  With the classes collapsed into one, there is probably some
  redundancy (maybe some methods can be simplified and/or removed),
  but we keep them this way for now.  /Sven
*/

/* These classes are based on the v1 RowsHeaderLen */
#undef ROWS_HEADER_LEN
#define ROWS_HEADER_LEN ROWS_HEADER_LEN_V1

/**
  @class Old_rows_log_event
  
  Base class for the three types of row-based events
  {Write|Update|Delete}_row_log_event_old, with event type codes
  PRE_GA_{WRITE|UPDATE|DELETE}_ROWS_EVENT.  These events are never
  created any more, except when reading a relay log created by an old
  server.
*/
class Old_rows_log_event : public Log_event
{
  /********** BEGIN CUT & PASTE FROM Rows_log_event **********/
public:
  /**
     Enumeration of the errors that can be returned.
   */
  enum enum_error
  {
    ERR_OPEN_FAILURE = -1,               /**< Failure to open table */
    ERR_OK = 0,                                 /**< No error */
    ERR_TABLE_LIMIT_EXCEEDED = 1,      /**< No more room for tables */
    ERR_OUT_OF_MEM = 2,                         /**< Out of memory */
    ERR_BAD_TABLE_DEF = 3,     /**< Table definition does not match */
    ERR_RBR_TO_SBR = 4  /**< daisy-chanining RBR to SBR not allowed */
  };

  /*
    These definitions allow you to combine the flags into an
    appropriate flag set using the normal bitwise operators.  The
    implicit conversion from an enum-constant to an integer is
    accepted by the compiler, which is then used to set the real set
    of flags.
  */
  enum enum_flag
  {
    /* Last event of a statement */
    STMT_END_F = (1U << 0),

    /* Value of the OPTION_NO_FOREIGN_KEY_CHECKS flag in thd->options */
    NO_FOREIGN_KEY_CHECKS_F = (1U << 1),

    /* Value of the OPTION_RELAXED_UNIQUE_CHECKS flag in thd->options */
    RELAXED_UNIQUE_CHECKS_F = (1U << 2),

    /** 
      Indicates that rows in this event are complete, that is contain
      values for all columns of the table.
     */
    COMPLETE_ROWS_F = (1U << 3)
  };

  typedef uint16 flag_set;

  /* Special constants representing sets of flags */
  enum 
  {
      RLE_NO_FLAGS = 0U
  };

  virtual ~Old_rows_log_event();

  void set_flags(flag_set flags_arg) { m_flags |= flags_arg; }
  void clear_flags(flag_set flags_arg) { m_flags &= ~flags_arg; }
  flag_set get_flags(flag_set flags_arg) const { return m_flags & flags_arg; }

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  virtual void pack_info(Protocol *protocol);
#endif

#ifdef MYSQL_CLIENT
  /* not for direct call, each derived has its own ::print() */
  virtual bool print(FILE *file, PRINT_EVENT_INFO *print_event_info)= 0;
#endif

#ifndef MYSQL_CLIENT
  int add_row_data(uchar *data, size_t length)
  {
    return do_add_row_data(data,length); 
  }
#endif

  /* Member functions to implement superclass interface */
  virtual int get_data_size();

  MY_BITMAP const *get_cols() const { return &m_cols; }
  size_t get_width() const          { return m_width; }
  ulonglong get_table_id() const    { return m_table_id; }

#ifndef MYSQL_CLIENT
  virtual bool write_data_header();
  virtual bool write_data_body();
  virtual const char *get_db() { return m_table->s->db.str; }
#endif
  /*
    Check that malloc() succeeded in allocating memory for the rows
    buffer and the COLS vector. Checking that an Update_rows_log_event_old
    is valid is done in the Update_rows_log_event_old::is_valid()
    function.
  */
  virtual bool is_valid() const
  {
    return m_rows_buf && m_cols.bitmap;
  }
  bool is_part_of_group() { return 1; }

  uint     m_row_count;         /* The number of rows added to the event */

protected:
  /* 
     The constructors are protected since you're supposed to inherit
     this class, not create instances of this class.
  */
#ifndef MYSQL_CLIENT
  Old_rows_log_event(THD*, TABLE*, ulonglong table_id,
                     MY_BITMAP const *cols, bool is_transactional);
#endif
  Old_rows_log_event(const uchar *row_data, uint event_len,
                     Log_event_type event_type,
                     const Format_description_log_event *description_event);

#ifdef MYSQL_CLIENT
  bool print_helper(FILE *, PRINT_EVENT_INFO *, char const *const name);
#endif

#ifndef MYSQL_CLIENT
  virtual int do_add_row_data(uchar *data, size_t length);
#endif

#ifndef MYSQL_CLIENT
  TABLE *m_table;		/* The table the rows belong to */
#endif
  ulonglong   m_table_id;	/* Table ID */
  MY_BITMAP   m_cols;		/* Bitmap denoting columns available */
  ulong       m_width;          /* The width of the columns bitmap */

  ulong       m_master_reclength; /* Length of record on master side */

  /* Bit buffers in the same memory as the class */
  my_bitmap_map  m_bitbuf[128/(sizeof(my_bitmap_map)*8)];
  my_bitmap_map  m_bitbuf_ai[128/(sizeof(my_bitmap_map)*8)];

  uchar    *m_rows_buf;		/* The rows in packed format */
  uchar    *m_rows_cur;		/* One-after the end of the data */
  uchar    *m_rows_end;		/* One-after the end of the allocated space */

  flag_set m_flags;		/* Flags for row-level events */

  /* helper functions */

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  const uchar *m_curr_row;     /* Start of the row being processed */
  const uchar *m_curr_row_end; /* One-after the end of the current row */
  uchar    *m_key;      /* Buffer to keep key value during searches */

  int find_row(rpl_group_info *);
  int write_row(rpl_group_info *, const bool);

  // Unpack the current row into m_table->record[0]
  int unpack_current_row(rpl_group_info *rgi)
  { 
    DBUG_ASSERT(m_table);
    ASSERT_OR_RETURN_ERROR(m_curr_row < m_rows_end, HA_ERR_CORRUPT_EVENT);
    return ::unpack_row(rgi, m_table, m_width, m_curr_row, &m_cols,
                                   &m_curr_row_end, &m_master_reclength, m_rows_end);
  }
#endif

private:

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  virtual int do_apply_event(rpl_group_info *rgi);
  virtual int do_update_pos(rpl_group_info *rgi);
  virtual enum_skip_reason do_shall_skip(rpl_group_info *rgi);

  /*
    Primitive to prepare for a sequence of row executions.

    DESCRIPTION

      Before doing a sequence of do_prepare_row() and do_exec_row()
      calls, this member function should be called to prepare for the
      entire sequence. Typically, this member function will allocate
      space for any buffers that are needed for the two member
      functions mentioned above.

    RETURN VALUE

      The member function will return 0 if all went OK, or a non-zero
      error code otherwise.
  */
  virtual 
  int do_before_row_operations(const Slave_reporting_capability *const log) = 0;

  /*
    Primitive to clean up after a sequence of row executions.

    DESCRIPTION
    
      After doing a sequence of do_prepare_row() and do_exec_row(),
      this member function should be called to clean up and release
      any allocated buffers.
      
      The error argument, if non-zero, indicates an error which happened during
      row processing before this function was called. In this case, even if 
      function is successful, it should return the error code given in the argument.
  */
  virtual 
  int do_after_row_operations(const Slave_reporting_capability *const log,
                              int error) = 0;

  /*
    Primitive to do the actual execution necessary for a row.

    DESCRIPTION
      The member function will do the actual execution needed to handle a row.
      The row is located at m_curr_row. When the function returns, 
      m_curr_row_end should point at the next row (one byte after the end
      of the current row).    

    RETURN VALUE
      0 if execution succeeded, 1 if execution failed.
      
  */
  virtual int do_exec_row(rpl_group_info *rgi) = 0;
#endif /* !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION) */

  /********** END OF CUT & PASTE FROM Rows_log_event **********/
 protected:
  
#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)

  int do_apply_event(Old_rows_log_event*, rpl_group_info *rgi);

  /*
    Primitive to prepare for a sequence of row executions.

    DESCRIPTION

      Before doing a sequence of do_prepare_row() and do_exec_row()
      calls, this member function should be called to prepare for the
      entire sequence. Typically, this member function will allocate
      space for any buffers that are needed for the two member
      functions mentioned above.

    RETURN VALUE

      The member function will return 0 if all went OK, or a non-zero
      error code otherwise.
  */
  virtual int do_before_row_operations(TABLE *table) = 0;

  /*
    Primitive to clean up after a sequence of row executions.

    DESCRIPTION
    
      After doing a sequence of do_prepare_row() and do_exec_row(),
      this member function should be called to clean up and release
      any allocated buffers.
  */
  virtual int do_after_row_operations(TABLE *table, int error) = 0;

  /*
    Primitive to prepare for handling one row in a row-level event.
    
    DESCRIPTION 

      The member function prepares for execution of operations needed for one
      row in a row-level event by reading up data from the buffer containing
      the row. No specific interpretation of the data is normally done here,
      since SQL thread specific data is not available: that data is made
      available for the do_exec function.

      A pointer to the start of the next row, or NULL if the preparation
      failed. Currently, preparation cannot fail, but don't rely on this
      behavior. 

    RETURN VALUE
      Error code, if something went wrong, 0 otherwise.
   */
  virtual int do_prepare_row(THD*, rpl_group_info*, TABLE*,
                             uchar const *row_start,
                             uchar const **row_end) = 0;

  /*
    Primitive to do the actual execution necessary for a row.

    DESCRIPTION
      The member function will do the actual execution needed to handle a row.

    RETURN VALUE
      0 if execution succeeded, 1 if execution failed.
      
  */
  virtual int do_exec_row(TABLE *table) = 0;

#endif /* !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION) */
};


/**
  @class Write_rows_log_event_old

  Old class for binlog events that write new rows to a table (event
  type code PRE_GA_WRITE_ROWS_EVENT).  Such events are never produced
  by this version of the server, but they may be read from a relay log
  created by an old server.  New servers create events of class
  Write_rows_log_event (event type code WRITE_ROWS_EVENT) instead.
*/
class Write_rows_log_event_old : public Old_rows_log_event
{
  /********** BEGIN CUT & PASTE FROM Write_rows_log_event **********/
public:
#if !defined(MYSQL_CLIENT)
  Write_rows_log_event_old(THD*, TABLE*, ulonglong table_id,
                           MY_BITMAP const *cols, bool is_transactional);
#endif
#ifdef HAVE_REPLICATION
  Write_rows_log_event_old(const uchar *buf, uint event_len,
                           const Format_description_log_event *description_event);
#endif
#if !defined(MYSQL_CLIENT) 
  static bool binlog_row_logging_function(THD *thd, TABLE *table,
                                          bool is_transactional,
                                          const uchar *before_record
                                          __attribute__((unused)),
                                          const uchar *after_record)
  {
    return thd->binlog_write_row(table, is_transactional, after_record);
  }
#endif

private:
#ifdef MYSQL_CLIENT
  bool print(FILE *file, PRINT_EVENT_INFO *print_event_info);
#endif

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  virtual int do_before_row_operations(const Slave_reporting_capability *const);
  virtual int do_after_row_operations(const Slave_reporting_capability *const,int);
  virtual int do_exec_row(rpl_group_info *);
#endif
  /********** END OF CUT & PASTE FROM Write_rows_log_event **********/

public:
  enum
  {
    /* Support interface to THD::binlog_prepare_pending_rows_event */
    TYPE_CODE = PRE_GA_WRITE_ROWS_EVENT
  };

private:
  virtual Log_event_type get_type_code() { return (Log_event_type)TYPE_CODE; }

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  // use old definition of do_apply_event()
  virtual int do_apply_event(rpl_group_info *rgi)
  { return Old_rows_log_event::do_apply_event(this, rgi); }

  // primitives for old version of do_apply_event()
  virtual int do_before_row_operations(TABLE *table);
  virtual int do_after_row_operations(TABLE *table, int error);
  virtual int do_prepare_row(THD*, rpl_group_info*, TABLE*,
                             uchar const *row_start, uchar const **row_end);
  virtual int do_exec_row(TABLE *table);

#endif
};


/**
  @class Update_rows_log_event_old

  Old class for binlog events that modify existing rows to a table
  (event type code PRE_GA_UPDATE_ROWS_EVENT).  Such events are never
  produced by this version of the server, but they may be read from a
  relay log created by an old server.  New servers create events of
  class Update_rows_log_event (event type code UPDATE_ROWS_EVENT)
  instead.
*/
class Update_rows_log_event_old : public Old_rows_log_event
{
  /********** BEGIN CUT & PASTE FROM Update_rows_log_event **********/
public:
#ifndef MYSQL_CLIENT
  Update_rows_log_event_old(THD*, TABLE*, ulonglong table_id,
                            MY_BITMAP const *cols,
                            bool is_transactional);
#endif

#ifdef HAVE_REPLICATION
  Update_rows_log_event_old(const uchar *buf, uint event_len,
                            const Format_description_log_event *description_event);
#endif

#if !defined(MYSQL_CLIENT) 
  static bool binlog_row_logging_function(THD *thd, TABLE *table,
                                          bool is_transactional,
                                          MY_BITMAP *cols,
                                          uint fields,
                                          const uchar *before_record,
                                          const uchar *after_record)
  {
    return thd->binlog_update_row(table, is_transactional,
                                  before_record, after_record);
  }
#endif

protected:
#ifdef MYSQL_CLIENT
  bool print(FILE *file, PRINT_EVENT_INFO *print_event_info);
#endif

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  virtual int do_before_row_operations(const Slave_reporting_capability *const);
  virtual int do_after_row_operations(const Slave_reporting_capability *const,int);
  virtual int do_exec_row(rpl_group_info *);
#endif /* !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION) */
  /********** END OF CUT & PASTE FROM Update_rows_log_event **********/

  uchar *m_after_image, *m_memory;
  
public:
  enum 
  {
    /* Support interface to THD::binlog_prepare_pending_rows_event */
    TYPE_CODE = PRE_GA_UPDATE_ROWS_EVENT
  };

private:
  virtual Log_event_type get_type_code() { return (Log_event_type)TYPE_CODE; }

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  // use old definition of do_apply_event()
  virtual int do_apply_event(rpl_group_info *rgi)
  { return Old_rows_log_event::do_apply_event(this, rgi); }

  // primitives for old version of do_apply_event()
  virtual int do_before_row_operations(TABLE *table);
  virtual int do_after_row_operations(TABLE *table, int error);
  virtual int do_prepare_row(THD*, rpl_group_info*, TABLE*,
                             uchar const *row_start, uchar const **row_end);
  virtual int do_exec_row(TABLE *table);
#endif /* !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION) */
};


/**
  @class Delete_rows_log_event_old

  Old class for binlog events that delete existing rows from a table
  (event type code PRE_GA_DELETE_ROWS_EVENT).  Such events are never
  produced by this version of the server, but they may be read from a
  relay log created by an old server.  New servers create events of
  class Delete_rows_log_event (event type code DELETE_ROWS_EVENT)
  instead.
*/
class Delete_rows_log_event_old : public Old_rows_log_event
{
  /********** BEGIN CUT & PASTE FROM Update_rows_log_event **********/
public:
#ifndef MYSQL_CLIENT
  Delete_rows_log_event_old(THD*, TABLE*, ulonglong,
                            MY_BITMAP const *cols, bool is_transactional);
#endif
#ifdef HAVE_REPLICATION
  Delete_rows_log_event_old(const uchar *buf, uint event_len,
                            const Format_description_log_event *description_event);
#endif
#if !defined(MYSQL_CLIENT) 
  static bool binlog_row_logging_function(THD *thd, TABLE *table,
                                          bool is_transactional,
                                          MY_BITMAP *cols,
                                          uint fields,
                                          const uchar *before_record,
                                          const uchar *after_record
                                          __attribute__((unused)))
  {
    return thd->binlog_delete_row(table, is_transactional, before_record);
  }
#endif
  
protected:
#ifdef MYSQL_CLIENT
  bool print(FILE *file, PRINT_EVENT_INFO *print_event_info);
#endif

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  virtual int do_before_row_operations(const Slave_reporting_capability *const);
  virtual int do_after_row_operations(const Slave_reporting_capability *const,int);
  virtual int do_exec_row(rpl_group_info *);
#endif
  /********** END CUT & PASTE FROM Delete_rows_log_event **********/

  uchar *m_after_image, *m_memory;
 
public:
  enum 
  {
    /* Support interface to THD::binlog_prepare_pending_rows_event */
    TYPE_CODE = PRE_GA_DELETE_ROWS_EVENT
  };

private:
  virtual Log_event_type get_type_code() { return (Log_event_type)TYPE_CODE; }

#if !defined(MYSQL_CLIENT) && defined(HAVE_REPLICATION)
  // use old definition of do_apply_event()
  virtual int do_apply_event(rpl_group_info *rgi)
  { return Old_rows_log_event::do_apply_event(this, rgi); }

  // primitives for old version of do_apply_event()
  virtual int do_before_row_operations(TABLE *table);
  virtual int do_after_row_operations(TABLE *table, int error);
  virtual int do_prepare_row(THD*, rpl_group_info*, TABLE*,
                             uchar const *row_start, uchar const **row_end);
  virtual int do_exec_row(TABLE *table);
#endif
};


#endif
