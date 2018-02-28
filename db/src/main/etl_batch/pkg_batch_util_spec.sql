CREATE OR REPLACE PACKAGE pack_batch_util
-- --------------------------------------------------------
IS
  
-- Purpose: Generic package specification for batches     

   TYPE timer_rectype IS RECORD (
      last_timing                   DATE,
      last_context                  VARCHAR2 (50));

   TYPE timer_tabtype IS TABLE OF timer_rectype
      INDEX BY BINARY_INTEGER;

   TYPE var_tabtype IS TABLE OF VARCHAR2 (120)
      INDEX BY BINARY_INTEGER;

   TYPE int_tabtype IS TABLE OF INTEGER
      INDEX BY BINARY_INTEGER;

   gr_batch_master        batch_master%ROWTYPE;
   gr_batch_monitor       batch_monitor%ROWTYPE;
   glo_exclusive_run_yn   batch_monitor.exclusive_run_yn%TYPE;
   gt_timer               timer_tabtype;
   glo_sysdate            DATE                                  := SYSDATE;
   glo_run_id             batch_monitor.run_id%TYPE             := 0;
   glo_called_by_forms    VARCHAR2 (1)                          := 'N';

   FUNCTION func_datediff (par_start_date IN DATE, par_end_date IN DATE)
      RETURN VARCHAR2;

   -- Capture current value using SYSDATE
   PROCEDURE proc_capture (par_context_in IN VARCHAR2 := NULL);

   -- Construct message showing time elapsed since call to capture 
   PROCEDURE proc_show_elapsed (
      par_prefix_in    IN   VARCHAR2 := NULL,
      par_context_in   IN   VARCHAR2 := NULL);

   FUNCTION func_batch_startup (
      par_batchname          IN   VARCHAR2,
      par_run_level          IN   INTEGER,
      par_exclusive_run_yn   IN   VARCHAR2,
      par_parameters         IN   VARCHAR2 DEFAULT NULL,
      par_called_by_shell    IN   VARCHAR2 DEFAULT 'N',
      par_called_by_forms    IN   VARCHAR2 DEFAULT 'N')
      RETURN INTEGER;

   PROCEDURE proc_batch_endup;

   PROCEDURE proc_batch_endup (
      par_status           IN   VARCHAR2,
      par_recs_processed   IN   INTEGER DEFAULT NULL,
      par_recs_in_error    IN   INTEGER DEFAULT NULL);

   PROCEDURE proc_batch_endup (
      par_status           IN   VARCHAR2,
      par_recs_processed   IN   INTEGER DEFAULT NULL,
      par_recs_in_error    IN   INTEGER DEFAULT NULL,
      pt_desc              IN   pack_batch_util.var_tabtype,
      pt_value             IN   pack_batch_util.int_tabtype);

   PROCEDURE proc_batch_continue (
      par_batchname   IN   VARCHAR2,
      par_run_level   IN   INTEGER,
      par_run_id      IN   INTEGER);

   FUNCTION func_check_date (
      par_run_date    IN OUT   VARCHAR2,
      par_error_msg   OUT      VARCHAR2)
      RETURN INTEGER;

   FUNCTION func_field_counter (
      par_source_string   IN       VARCHAR2,
      par_unterminated    IN       BOOLEAN DEFAULT FALSE,
      par_delimiter       IN       VARCHAR2 DEFAULT ',',
      par_count           OUT      INTEGER)
      RETURN INTEGER;

   FUNCTION func_get_nth_field (
      par_source_string    IN       VARCHAR2,
      par_unterminated     IN       BOOLEAN DEFAULT FALSE,
      par_delimiter        IN       VARCHAR2 DEFAULT ',',
      par_field_position   IN       NUMBER,
      par_out_val          OUT      VARCHAR2)
      RETURN INTEGER;

   FUNCTION func_string_to_number (par_string IN VARCHAR2)
      RETURN NUMBER;

   FUNCTION func_atol (par_val IN VARCHAR2)
      RETURN INTEGER;

   FUNCTION func_get_env_from_db (par_var_name IN VARCHAR2)
      RETURN VARCHAR2;

   FUNCTION func_daily000 (
      par_run_date           IN   VARCHAR2,
      par_frequency          IN   VARCHAR2,
      par_run_level          IN   INTEGER DEFAULT NULL,
      par_exclusive_run_yn   IN   VARCHAR2 DEFAULT 'N',
      par_flag               IN   INTEGER DEFAULT NULL)
      RETURN INTEGER;

   FUNCTION func_get_run_command (par_batch_name IN VARCHAR2)
      RETURN VARCHAR2;

   PRAGMA restrict_references (func_get_run_command, WNDS);

   PROCEDURE proc_send_mail_group (
      par_recipient   IN   VARCHAR2,
      par_subj        IN   VARCHAR2,
      par_body        IN   VARCHAR2);

   -- It obtains the data_file names of a particular batch stored in tmp_run_loader table. Data file names
   -- are seperated with blank spaces.
   FUNCTION func_get_loader_file_name(
      par_batch_name   IN  VARCHAR2,
      par_run_day      IN  VARCHAR2
   )
   RETURN VARCHAR2;

   PROCEDURE proc_write_session_longops (
      par_op_name   IN   VARCHAR2,
      par_sofar     IN   NUMBER DEFAULT NULL);
END pack_batch_util;
/
SHOW error
GRANT     EXECUTE ON pack_batch_util TO PUBLIC;
