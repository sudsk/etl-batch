CREATE OR REPLACE PACKAGE BODY pkg_batch_util
-- --------------------------------------------------------
IS
--
--
   SUBTYPE procedurename IS batch_log.procedure_name%TYPE;

   glo_b_statement_num              batch_log.statement_num%TYPE;
   glo_b_package_name               batch_log.package_name%TYPE
                                                         := 'pack_batch_util';
   e_no_record_batch_master         EXCEPTION;
   e_too_many_record_batch_master   EXCEPTION;
   glo_b_control_date               DATE;

-- Function to calculate time difference between two dates and 
-- return it in HH24:MI:SS format 
   FUNCTION func_datediff (par_start_date IN DATE, par_end_date IN DATE)
      RETURN VARCHAR2
   IS
      loc_secs          INTEGER
                            := (par_end_date - par_start_date) * 24 * 60 * 60;
      loc_mins          INTEGER      := 0;
      loc_hours         INTEGER      := 0;
      loc_date_format   VARCHAR2 (8);
   BEGIN
      IF loc_secs < 0
      THEN
         RETURN 'INVALID DATE';
      END IF;

      loc_hours := TRUNC (loc_secs / 3600);
      loc_mins := TRUNC ((loc_secs MOD 3600) / 60);
      loc_secs := loc_secs MOD 60;
      loc_date_format := loc_hours || ':' || loc_mins || ':' || loc_secs;
      RETURN loc_date_format;
   END func_datediff;

-- Save current time and context to global pl/sql table.
   PROCEDURE proc_capture (par_context_in IN VARCHAR2 := NULL)
   IS
      loc_last   BINARY_INTEGER;
   BEGIN
      loc_last := NVL (gt_timer.LAST, 0);
      loc_last := loc_last + 1;
      gt_timer (loc_last).last_timing := SYSDATE;
      gt_timer (loc_last).last_context := par_context_in;
   END proc_capture;

-- Construct message for display of elapsed time. Programmer can
-- include a prefix to the message. 
   PROCEDURE proc_show_elapsed (
      par_prefix_in    IN   VARCHAR2 := NULL,
      par_context_in   IN   VARCHAR2 := NULL)
   IS
      loc_return_value   VARCHAR2 (500);
      loc_time_elapsed   VARCHAR2 (20);
      loc_last_timing    DATE           := NULL;
      loc_last_context   VARCHAR2 (50)  := NULL;
   BEGIN
      FOR i IN 1 .. NVL (gt_timer.LAST, 0)
      LOOP
         IF (UPPER (par_context_in) = UPPER (gt_timer (i).last_context))
         THEN
            loc_last_timing := gt_timer (i).last_timing;
            loc_last_context := par_context_in;
         END IF;
      END LOOP;

      IF loc_last_timing IS NULL
      THEN
         loc_return_value := NULL;
      ELSIF par_prefix_in IS NULL
      THEN
         loc_time_elapsed :=
            func_datediff (par_start_date      => loc_last_timing,
                           par_end_date        => SYSDATE);
         loc_return_value := 'Total Time Taken ' || loc_time_elapsed;
      ELSE
         loc_time_elapsed :=
            func_datediff (par_start_date      => loc_last_timing,
                           par_end_date        => SYSDATE);
         loc_return_value := par_prefix_in || loc_time_elapsed;
      END IF;

      pack_exception.proc_reclog (loc_return_value);
   END proc_show_elapsed;

-- Function to check for NULL run_date or incorrect length . If everything is fine
-- then reformat in DD-MON-YYYY
   FUNCTION func_check_date (
      par_run_date    IN OUT   VARCHAR2,
      par_error_msg   OUT      VARCHAR2)
      RETURN INTEGER
   IS
   BEGIN
      IF    LENGTH (par_run_date) < 10
         OR par_run_date IS NULL
      THEN
         par_error_msg :=
                'Invalid Date <'
             || par_run_date
             || '> Correct Usage For Date : DD-MON-YYYY';
         RETURN (-1);
      END IF;

      IF LENGTH (par_run_date) > 11
      THEN
         par_error_msg :=
                'Invalid Date <'
             || par_run_date
             || '> Correct Usage For  Date : DD-MON-YYYY';
         RETURN (1);
      END IF;

      par_run_date :=
                TO_CHAR (TO_DATE (par_run_date, 'DD-MON-YYYY'), 'DD-MON-YYYY');
      RETURN 0;
   EXCEPTION
      WHEN OTHERS
      THEN
         par_error_msg := 'Invalid Date <' || par_run_date || '> ' || SQLERRM;
         RETURN 2;
   END func_check_date;

-- Procedure to get all the information about the batch which is undergoing 
-- processing from batch_master table and store it in a pl/sql record.
   PROCEDURE proc_get_module_info (
      par_batchname   IN   VARCHAR2,
      par_run_level   IN   INTEGER)
   IS
      loc_module_id   INTEGER;
   BEGIN
      SELECT *
        INTO gr_batch_master
        FROM batch_master
       WHERE UPPER (module_name) = UPPER (par_batchname)
         AND (   (    par_run_level IS NOT NULL
                  AND run_level = par_run_level)
              OR     par_run_level IS NULL
                 AND run_level =
                           (SELECT MIN (run_level)
                              FROM batch_master
                             WHERE UPPER (module_name) = UPPER (par_batchname)));
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RAISE e_no_record_batch_master;
      WHEN TOO_MANY_ROWS
      THEN
         RAISE e_too_many_record_batch_master;
   END proc_get_module_info;

-- Procedure to get all the transaction info about the batch which is undergoing 
-- processing from batch_monitor table and store it in a pl/sql record.
   PROCEDURE proc_get_transaction_info
   IS
   BEGIN
      SELECT   *
          INTO gr_batch_monitor
          FROM batch_monitor
         WHERE module_id = gr_batch_master.module_id
           AND run_id = glo_run_id
           AND run_status = 'RUNNING'
           AND ROWNUM < 2
      ORDER BY run_date DESC;
   END;

-- Function to get the max run_id on the current date for a particular batch
-- and then increment it by 1 for the next run.
   FUNCTION func_get_run_id
      RETURN INTEGER
   IS
      loc_run_id   INTEGER;
   BEGIN
      SELECT NVL (MAX (run_id), 0)
        INTO loc_run_id
        FROM batch_monitor
       WHERE module_id = gr_batch_master.module_id
         AND TRUNC (run_date) = TRUNC (glo_sysdate);

      RETURN loc_run_id + 1;
   END func_get_run_id;

-- Procedure to insert a record in the batch_monitor table whenever a batch starts.
   PROCEDURE proc_insert_batch_monitor (
      par_module_id    IN   batch_monitor.module_id%TYPE DEFAULT 0,
      par_run_id       IN   batch_monitor.run_id%TYPE DEFAULT 0,
      par_run_status   IN   batch_monitor.run_status%TYPE,
      par_sub_system   IN   batch_monitor.sub_system%TYPE DEFAULT NULL,
      par_parameters   IN   batch_monitor.PARAMETERS%TYPE)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      INSERT INTO batch_monitor
                  (module_id,
                   run_date,
                   run_id,
                   PARAMETERS,
                   audit_id,
                   run_status,
                   sub_system,
                   exclusive_run_yn,
                   control_date)
           VALUES (par_module_id,
                   glo_sysdate,
                   par_run_id,
                   par_parameters,
                   pack_session_variables.glo_loginid,
                   par_run_status,
                   par_sub_system,
                   glo_exclusive_run_yn,
                   glo_b_control_date);

      COMMIT;
   END proc_insert_batch_monitor;

-- Function to disallow a second run of a batch with same parameters while it is
-- still running.
   FUNCTION func_duplicate_run_chk
      RETURN INTEGER
   IS
      loc_duplicate_run_chk   INTEGER := 0;
   BEGIN
      SELECT 1
        INTO loc_duplicate_run_chk
        FROM batch_monitor
       WHERE module_id = gr_batch_master.module_id
         AND NVL (PARAMETERS, ' ') =
                                NVL (pack_exception.glo_parameter_string, ' ')
         AND NVL (TRUNC (run_date), SYSDATE + 1) =
                (SELECT NVL (TRUNC (MAX (run_date)), SYSDATE + 1)
                   FROM batch_monitor
                  WHERE module_id = gr_batch_master.module_id
                    AND NVL (PARAMETERS, ' ') =
                                NVL (pack_exception.glo_parameter_string, ' ')
                    AND run_status = 'RUNNING')
         AND run_status = 'RUNNING';

      RETURN loc_duplicate_run_chk;
   EXCEPTION
      WHEN NO_DATA_FOUND
      THEN
         RETURN 0;
      WHEN OTHERS
      THEN
         RETURN 1;
   END func_duplicate_run_chk;

-- Function to check dependency of a batch on its parent batches from
-- batch_dependency table.
   FUNCTION func_dependency_chk
      RETURN INTEGER
   IS
      loc_parent_run_status   INTEGER (1)                     := 0;
      loc_first_time          BOOLEAN                         := TRUE;
      loc_module_name         batch_master.module_name%TYPE;
   BEGIN
      FOR rec_batch_dependency IN (SELECT parent_module_id,
                                          dependency_type
                                     FROM batch_dependency
                                    WHERE child_id = gr_batch_master.module_id)
      LOOP
         BEGIN
            SELECT module_name
              INTO loc_module_name
              FROM batch_master
             WHERE module_id = rec_batch_dependency.parent_module_id;

            LOOP
               BEGIN
                  SELECT DECODE (run_status,
                                 'SUCCESS', 0,
                                 'RUNNING', 1,
                                 'WAITING', 1,
                                 DECODE (rec_batch_dependency.dependency_type,
                                         'MANDATORY', 2,
                                         'OPTIONAL', 0,
                                         'WAIT', 1,
                                         3))
                    INTO loc_parent_run_status
                    FROM batch_monitor
                   WHERE TRUNC (control_date) = TRUNC (glo_b_control_date)
                     AND run_id =
                            (SELECT NVL (MAX (run_id), 1)
                               FROM batch_monitor
                              WHERE TRUNC (control_date) =
                                                    TRUNC (glo_b_control_date)
                                AND module_id =
                                         rec_batch_dependency.parent_module_id
                                AND (   (    UPPER (SUBSTR (PARAMETERS,
                                                            1,
                                                               INSTR (PARAMETERS,
                                                                      'Run_level=<',
                                                                      1)
                                                             - 2)) =
                                                UPPER (SUBSTR (pack_exception.glo_parameter_string,
                                                               1,
                                                                  INSTR (pack_exception.glo_parameter_string,
                                                                         'Run_level=<',
                                                                         1)
                                                                - 2))
                                         AND gr_batch_master.module_name =
                                                               loc_module_name)
                                     OR gr_batch_master.module_name !=
                                                               loc_module_name))
                     AND (   (    UPPER (SUBSTR (PARAMETERS,
                                                 1,
                                                    INSTR (PARAMETERS,
                                                           'Run_level=<',
                                                           1)
                                                  - 2)) =
                                     UPPER (SUBSTR (pack_exception.glo_parameter_string,
                                                    1,
                                                       INSTR (pack_exception.glo_parameter_string,
                                                              'Run_level=<',
                                                              1)
                                                     - 2))
                              AND gr_batch_master.module_name =
                                                               loc_module_name)
                          OR gr_batch_master.module_name != loc_module_name)
                     AND module_id = rec_batch_dependency.parent_module_id
                     AND ROWNUM = 1;

                  EXIT WHEN loc_parent_run_status != 1;
                  DBMS_LOCK.sleep (120);
               EXCEPTION
                  WHEN NO_DATA_FOUND
                  THEN
                     DBMS_LOCK.sleep (120);
               END;
            END LOOP;

            EXIT WHEN loc_parent_run_status = 2;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;
         END;
      END LOOP;

      RETURN loc_parent_run_status;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN 1;
   END func_dependency_chk;

-- Populate session variables
   PROCEDURE proc_set_session_vars (
      par_batchname   IN   batch_log.batch_name%TYPE)
   IS
      loc_username   VARCHAR2 (20);
      loc_ret_env    envvar.VALUE%TYPE   := NULL;
   BEGIN
      BEGIN
         loc_ret_env :=
                   pack_envvar.func_get_env (par_var_name      => 'BATCH_FLG_DBG');

         IF (loc_ret_env = 'Y')
         THEN
            pack_session_variables.glo_debug := 1;
         ELSE
            pack_session_variables.glo_debug := 0;
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            pack_session_variables.glo_debug := 0;
      END;

      BEGIN
         loc_ret_env :=
                   pack_envvar.func_get_env (par_var_name      => 'BATCH_FLG_LOG');

         IF (loc_ret_env = 'Y')
         THEN
            pack_session_variables.glo_log_status := 1;
         ELSE
            pack_session_variables.glo_log_status := 0;
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            pack_session_variables.glo_log_status := 0;
      END;

      BEGIN
         loc_ret_env :=
                   pack_envvar.func_get_env (par_var_name      => 'BATCH_FLG_ERR');

         IF (loc_ret_env = 'Y')
         THEN
            pack_session_variables.glo_err_status := 1;
         ELSE
            pack_session_variables.glo_err_status := 0;
         END IF;
      EXCEPTION
         WHEN OTHERS
         THEN
            pack_session_variables.glo_err_status := 0;
      END;

      pack_session_variables.glo_runenv := 'C';
      pack_session_variables.glo_batchflag := 'Y';
      pack_session_variables.glo_context := par_batchname;
      loc_username :=
                 REPLACE (REPLACE (USER, 'OPS$', ''), 'WEB_OWNER', 'WEB_USER');
      pack_session_variables.proc_setloginid (par_id => loc_username);

      BEGIN
         glo_b_control_date :=
            TO_DATE (pack_batch_util.func_get_env_from_db (par_var_name      => 'BATCH_CONTROL_DATE'),
                     'DD-MON-YYYY');
      EXCEPTION
         WHEN OTHERS
         THEN
            glo_b_control_date := TRUNC (SYSDATE);
      END;
   END proc_set_session_vars;

-- Procedure to update the record in batch_monitor table 
   PROCEDURE proc_update_batch_monitor (
      par_status   IN   batch_monitor.run_status%TYPE,
      par_run_id   IN   batch_monitor.run_id%TYPE)
   IS
      loc_run_status   batch_monitor.run_status%TYPE   := 'RUNNING';
      loc_run_id       batch_monitor.run_id%TYPE       := par_run_id;
      loc_sysdate      batch_monitor.run_date%TYPE     := glo_sysdate;
   BEGIN
      IF (par_status = 'RUNNING')
      THEN
         loc_run_status := 'WAITING';
         loc_run_id := 0;
         loc_sysdate := glo_sysdate;
         glo_sysdate := SYSDATE;
      END IF;

      UPDATE batch_monitor
         SET run_status = par_status,
             run_date = glo_sysdate,
             run_id = par_run_id
       WHERE module_id = gr_batch_master.module_id
         AND run_date = loc_sysdate
         AND run_id = loc_run_id
         AND run_status = loc_run_status
         AND PARAMETERS = pack_exception.glo_parameter_string;
   END proc_update_batch_monitor;

-- Procedure to update the record in batch_monitor table whenever a batch 
-- finishes processing .
   PROCEDURE proc_update_batch_monitor (
      par_status           IN   VARCHAR2,
      par_recs_processed   IN   INTEGER,
      par_recs_in_error    IN   INTEGER)
   IS
   BEGIN
      UPDATE batch_monitor
         SET run_status = par_status,
             end_time = SYSDATE,
             records_processed = par_recs_processed,
             records_in_error = par_recs_in_error
       WHERE module_id = gr_batch_master.module_id
         AND run_date = glo_sysdate
         AND run_id = glo_run_id
         AND run_status IN ('RUNNING', 'WAITING')
         AND PARAMETERS = pack_exception.glo_parameter_string;
   END proc_update_batch_monitor;

-- A startup procedure for every batch which setup the main timer,
-- populates global variables of pack_session_variables and call
-- proc_insert_batch_monitor to insert a record in batch-monitor table. 
   FUNCTION func_batch_startup (
      par_batchname          IN   VARCHAR2,
      par_run_level          IN   INTEGER,
      par_exclusive_run_yn   IN   VARCHAR2,
      par_parameters         IN   VARCHAR2 DEFAULT NULL,
      par_called_by_shell    IN   VARCHAR2 DEFAULT 'N',
      par_called_by_forms    IN   VARCHAR2 DEFAULT 'N')
      RETURN INTEGER
   IS
      e_batch_disabled    EXCEPTION;
      e_duplicate_run     EXCEPTION;
      e_dependency_fail   EXCEPTION;
      loc_retval          INTEGER   := 0;
   BEGIN
      DBMS_APPLICATION_INFO.set_module (module_name      => par_batchname,
                                        action_name      => 'STARTED');
      glo_called_by_forms := par_called_by_forms;

      IF (NVL (glo_called_by_forms, 'N') = 'N')
      THEN
         glo_exclusive_run_yn := par_exclusive_run_yn;
         pack_exception.glo_parameter_string :=
                      par_parameters || ' Run_level=<' || par_run_level
                      || '>';
         proc_set_session_vars (par_batchname => par_batchname);
         proc_get_module_info (par_batchname, par_run_level);

         IF gr_batch_master.disabled_date IS NOT NULL
         THEN
            RAISE e_batch_disabled;
         END IF;

         loc_retval := func_duplicate_run_chk;

         IF (loc_retval != 0)
         THEN
            RAISE e_duplicate_run;
         END IF;

         IF (glo_exclusive_run_yn = 'Y')
         THEN
            glo_sysdate := SYSDATE;
            proc_insert_batch_monitor (par_module_id       => gr_batch_master.module_id,
                                       par_run_id          => 0,
                                       par_run_status      => 'WAITING',
                                       par_sub_system      => gr_batch_master.sub_system,
                                       par_parameters      => pack_exception.glo_parameter_string);
            DBMS_APPLICATION_INFO.set_action (action_name => 'WAITING');
            loc_retval := func_dependency_chk;

            IF (loc_retval != 0)
            THEN
               RAISE e_dependency_fail;
            END IF;

            glo_run_id := func_get_run_id;
            proc_update_batch_monitor (par_status      => 'RUNNING',
                                       par_run_id      => glo_run_id);
            COMMIT;
         ELSE
            glo_sysdate := SYSDATE;
            glo_run_id := func_get_run_id;
            proc_insert_batch_monitor (par_module_id       => gr_batch_master.module_id,
                                       par_run_id          => glo_run_id,
                                       par_run_status      => 'RUNNING',
                                       par_sub_system      => gr_batch_master.sub_system,
                                       par_parameters      => pack_exception.glo_parameter_string);
         END IF;

         DBMS_APPLICATION_INFO.set_action (action_name => 'RUNNING');
      END IF;

      RETURN glo_run_id;
   EXCEPTION
      WHEN e_too_many_record_batch_master
      THEN
         proc_insert_batch_monitor (par_module_id       => gr_batch_master.module_id,
                                    par_run_status      => 'TOO_MANY_RECORDS_BATCH_MASTER',
                                    par_sub_system      => gr_batch_master.sub_system,
                                    par_parameters      => pack_exception.glo_parameter_string);

         IF (par_called_by_shell = 'Y')
         THEN
            RETURN (0);
         ELSE
            RAISE;
         END IF;
      WHEN e_no_record_batch_master
      THEN
         proc_insert_batch_monitor (par_module_id       => 0,
                                    par_run_status      => 'NO_RECORD_BATCH_MASTER',
                                    par_sub_system      => NULL,
                                    par_parameters      =>    'BatchName=<'
                                                           || par_batchname
                                                           || '> '
                                                           || pack_exception.glo_parameter_string);

         IF (par_called_by_shell = 'Y')
         THEN
            RETURN (0);
         ELSE
            RAISE;
         END IF;
      WHEN e_batch_disabled
      THEN
         proc_insert_batch_monitor (par_module_id       => gr_batch_master.module_id,
                                    par_run_status      => 'BATCH-DISABLED',
                                    par_sub_system      => gr_batch_master.sub_system,
                                    par_parameters      => pack_exception.glo_parameter_string);

         IF (par_called_by_shell = 'Y')
         THEN
            RETURN (0);
         ELSE
            RAISE;
         END IF;
      WHEN e_duplicate_run
      THEN
         proc_insert_batch_monitor (par_module_id       => gr_batch_master.module_id,
                                    par_run_status      => 'RE-RUN FAILURE',
                                    par_sub_system      => gr_batch_master.sub_system,
                                    par_parameters      => pack_exception.glo_parameter_string);

         IF (par_called_by_shell = 'Y')
         THEN
            RETURN (0);
         ELSE
            RAISE;
         END IF;
      WHEN e_dependency_fail
      THEN
         proc_update_batch_monitor (par_status              => 'DEPENDENCY FAILURE',
                                    par_recs_processed      => 0,
                                    par_recs_in_error       => 0);
         COMMIT;

         IF (par_called_by_shell = 'Y')
         THEN
            RETURN (0);
         ELSE
            RAISE;
         END IF;
      WHEN OTHERS
      THEN
         pack_exception.proc_recngo (par_package_name        => 'pack_batch_util',
                                     par_procedure_name      => 'func_batch_startup',
                                     par_statement_num       => 0,
                                     par_error_desc          => SQLERRM);

         IF (par_called_by_shell = 'Y')
         THEN
            RETURN (0);
         ELSE
            RAISE;
         END IF;
   END func_batch_startup;

-- A continue procedure for batches which requires to end a session
-- midway between and again restart the batch from that point onwards

   PROCEDURE proc_batch_continue (
      par_batchname   IN   VARCHAR2,
      par_run_level   IN   INTEGER,
      par_run_id      IN   INTEGER)
   IS
   BEGIN
      glo_run_id := par_run_id;
      proc_set_session_vars (par_batchname => par_batchname);
      proc_get_module_info (par_batchname      => par_batchname,
                            par_run_level      => par_run_level);
      proc_get_transaction_info;
      pack_exception.glo_parameter_string := gr_batch_monitor.PARAMETERS;
      glo_sysdate := gr_batch_monitor.run_date;
   END proc_batch_continue;

-- A endup procedure for reports only which calls proc_update_batch_monitor
-- to update batch_monitor table , calls proc_commit_batchlog to insert 
-- the leftover records in batch_log and resets glo_batchflag and bailout flag.
   PROCEDURE proc_batch_endup
   IS
   BEGIN
      IF (NVL (glo_called_by_forms, 'N') = 'N')
      THEN
         pack_batch_util.proc_update_batch_monitor (par_status              => 'SUCCESS',
                                                    par_recs_processed      => NULL,
                                                    par_recs_in_error       => NULL);
         pack_exception.proc_commit_batchlog;
         pack_session_variables.glo_batchflag := 'N';
         pack_exception.glo_write_once := FALSE;
         DBMS_APPLICATION_INFO.set_module (module_name      => NULL,
                                           action_name      => NULL);
      END IF;
   END proc_batch_endup;

-- A endup procedure for process batch which calls proc_update_batch_monitor
-- to update batch_monitor table , records the main timer timings in batch_log,
-- calls proc_commit_batchlog to insert the leftover records in batch_log
-- and resets glo_batchflag and bailout flag.

   PROCEDURE proc_batch_endup (
      par_status           IN   VARCHAR2,
      par_recs_processed   IN   INTEGER DEFAULT NULL,
      par_recs_in_error    IN   INTEGER DEFAULT NULL)
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      IF (NVL (glo_called_by_forms, 'N') = 'N')
      THEN
         pack_batch_util.proc_update_batch_monitor (par_status              => par_status,
                                                    par_recs_processed      => par_recs_processed,
                                                    par_recs_in_error       => par_recs_in_error);
         COMMIT;
         pack_exception.proc_commit_batchlog;
         pack_session_variables.glo_batchflag := 'N';
         pack_exception.glo_write_once := FALSE;
         DBMS_APPLICATION_INFO.set_module (module_name      => NULL,
                                           action_name      => NULL);
      ELSE
         COMMIT;
      END IF;
   END proc_batch_endup;

-- Overloaded proc_batch_endup with two extra parameters .The first one takes
-- descriptions of log_messages and the second parameter holds the value in
-- pl/sql table
   PROCEDURE proc_batch_endup (
      par_status           IN   VARCHAR2,
      par_recs_processed   IN   INTEGER DEFAULT NULL,
      par_recs_in_error    IN   INTEGER DEFAULT NULL,
      pt_desc              IN   pack_batch_util.var_tabtype,
      pt_value             IN   pack_batch_util.int_tabtype)
   IS
   BEGIN
      IF (NVL (glo_called_by_forms, 'N') = 'N')
      THEN
         IF     pt_desc.COUNT <> 0
            AND pt_value.COUNT <> 0
         THEN
            FOR loc_i IN pt_desc.FIRST .. pt_desc.LAST
            LOOP
               pack_exception.proc_reclog (   pt_desc (loc_i)
                                           || ':    '
                                           || TO_CHAR (pt_value (loc_i)));
            END LOOP;
         END IF;

         proc_batch_endup (par_status              => par_status,
                           par_recs_processed      => par_recs_processed,
                           par_recs_in_error       => par_recs_in_error);
      END IF;
   END proc_batch_endup;

-- Function for separating fields from a delimited string where delimiter may be
-- anything like comma, space or anything else and also the string may or may
-- not be terminated/ended with the delimiter.
-- The function func_field_counter returns the count of the no. of fields in the
-- delimited string
   FUNCTION func_field_counter (
      par_source_string   IN       VARCHAR2,
      par_unterminated    IN       BOOLEAN DEFAULT FALSE,
      par_delimiter       IN       VARCHAR2 DEFAULT ',',
      par_count           OUT      INTEGER)
      RETURN INTEGER
   IS
      loc_modifier   PLS_INTEGER     := 0;
      loc_old_size   PLS_INTEGER     := LENGTH (par_source_string);
      loc_db3        PLS_INTEGER; -- number of fields in the source string
      loc_db2        NUMBER; -- length changed to number
      loc_db1        VARCHAR2 (2000); -- source string without delimeters
      loc_procname   procedurename   := 'func_counter';
   BEGIN
      IF par_unterminated
      THEN
         loc_modifier := 1;
      END IF;

      loc_db1 := REPLACE (par_source_string, par_delimiter);
      loc_db2 := LENGTH (loc_db1);
      loc_db3 := (loc_old_size - loc_db2) + loc_modifier;
      par_count := loc_db3;
      RETURN (0);
   EXCEPTION
      WHEN OTHERS
      THEN
         pack_exception.proc_recngo (par_package_name        => glo_b_package_name,
                                     par_procedure_name      => loc_procname,
                                     par_statement_num       => glo_b_statement_num,
                                     par_error_desc          => SQLERRM,
                                     par_message_string      => NULL);
         RETURN (1);
   END func_field_counter;

-- Function for separating fields from a delimited string where delimiter may be
-- anything like comma, space or anything else and also the string may or may
-- not be terminated/ended with the delimiter.
-- func_get_nth_field function is to get the n-th field in the string.
   FUNCTION func_get_nth_field (
      par_source_string    IN       VARCHAR2,
      par_unterminated     IN       BOOLEAN DEFAULT FALSE,
      par_delimiter        IN       VARCHAR2 DEFAULT ',',
      par_field_position   IN       NUMBER,
      par_out_val          OUT      VARCHAR2)
      RETURN INTEGER
   IS
      loc_ptr_end           PLS_INTEGER     := 0;
      loc_ptr_start         PLS_INTEGER     := 0;
      loc_source_str_copy   VARCHAR2 (2000) := par_source_string;
      loc_procname          procedurename   := 'func_get_string';
   BEGIN
      IF par_unterminated
      THEN
         loc_source_str_copy := loc_source_str_copy || par_delimiter;
      END IF;

      IF (par_field_position > 1)
      THEN
         loc_ptr_start :=
               INSTR (loc_source_str_copy,
                      par_delimiter,
                      1,
                       par_field_position - 1)
             + LENGTH (par_delimiter);
      ELSE
         loc_ptr_start := 1;
      END IF;

      loc_ptr_end :=
             INSTR (loc_source_str_copy, par_delimiter, 1, par_field_position);
      par_out_val :=
         SUBSTR (loc_source_str_copy,
                 loc_ptr_start,
                 (loc_ptr_end - loc_ptr_start));
      RETURN (0);
   EXCEPTION
      WHEN OTHERS
      THEN
         pack_exception.proc_recngo (par_package_name        => glo_b_package_name,
                                     par_procedure_name      => loc_procname,
                                     par_statement_num       => glo_b_statement_num,
                                     par_error_desc          => SQLERRM,
                                     par_message_string      => NULL);
         RETURN (1);
   END func_get_nth_field;

-- Function to replicate atoi() and atol() of pro*c
   FUNCTION func_string_to_number (par_string IN VARCHAR2)
      RETURN NUMBER
   IS
      loc_procedure_name   VARCHAR2 (30)  := 'FUNC_STRING_TO_NUMBER';
      loc_string           VARCHAR2 (100);
      loc_ret_num          NUMBER         := 0;
      loc_token            VARCHAR2 (1);
      loc_count            NUMBER         := 0;
   BEGIN
      loc_string := RTRIM (LTRIM (par_string));
      glo_b_statement_num := 100;

      WHILE (NVL (LENGTH (SUBSTR (loc_string, loc_count + 1, 1)), 0) > 0)
       AND (ASCII (SUBSTR (loc_string, loc_count + 1, 1)) BETWEEN 48 AND 57)
      LOOP
         glo_b_statement_num := 101;
         loc_token := SUBSTR (loc_string, loc_count + 1, 1);
         glo_b_statement_num := 102;
         loc_ret_num := 10 * loc_ret_num + ASCII (loc_token) - ASCII ('0');
         loc_count := loc_count + 1;
         loc_token := NULL;
      END LOOP;

      RETURN loc_ret_num;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN (0);
   END func_string_to_number;

-- Function to replicate atoi() and atol() of pro*c
   FUNCTION func_atol (par_val IN VARCHAR2)
      RETURN INTEGER
   IS
   BEGIN
      RETURN (func_string_to_number (par_val));
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN (0);
   END func_atol;

-- Overloaded Function to call pack_envvar.func_get_env after emptying pl/sql 
-- table holding environment variables. 
   FUNCTION func_get_env_from_db (par_var_name IN VARCHAR2)
      RETURN VARCHAR2
   IS
      loc_retval     envvar.VALUE%TYPE   := NULL;
      loc_procname   procedurename       := 'func_get_env_from_db';
   BEGIN
      pack_envvar.global_var_array.DELETE;
      loc_retval :=
            pack_generic_routines.func_get_env (par_var_name      => par_var_name);
      RETURN loc_retval;
   EXCEPTION
      WHEN OTHERS
      THEN
         RAISE;
   END func_get_env_from_db;

   FUNCTION func_daily000 (
      par_run_date           IN   VARCHAR2,
      par_frequency          IN   VARCHAR2,
      par_run_level          IN   INTEGER DEFAULT NULL,
      par_exclusive_run_yn   IN   VARCHAR2 DEFAULT 'N',
      par_flag               IN   INTEGER DEFAULT NULL)
      RETURN INTEGER
   IS
      e_null_date_passed   EXCEPTION;
      loc_run_id           batch_monitor.run_id%TYPE;
      loc_err_msg          VARCHAR2 (200);
      loc_retval           PLS_INTEGER                 := 0;
      loc_sleep_time       PLS_INTEGER                 := 0;
      loc_run_date         VARCHAR2 (20)               := par_run_date;
      loc_procname         procedurename               := 'func_daily000';
   BEGIN
      glo_b_statement_num := 501;
      loc_run_id :=
         pack_batch_util.func_batch_startup (par_batchname             => 'DAILY000',
                                             par_run_level             => par_run_level,
                                             par_exclusive_run_yn      => par_exclusive_run_yn,
                                             par_parameters            =>    'par_run_date=<'
                                                                          || par_run_date
                                                                          || '> par_frequency=<'
                                                                          || par_frequency
                                                                          || '> par_flag=<'
                                                                          || par_flag
                                                                          || '>');
      glo_b_statement_num := 502;
      loc_retval :=
         pack_batch_util.func_check_date (par_run_date       => loc_run_date,
                                          par_error_msg      => loc_err_msg);

      IF (loc_retval != 0)
      THEN
         RAISE e_null_date_passed;
      END IF;

      glo_b_statement_num := 503;

      IF (par_flag IS NULL)
      THEN
         pack_envvar.proc_upd_env (par_variable_name      => 'BATCH_CONTROL_DATE',
                                   par_scope              => 'GLOBAL',
                                   par_value              => par_run_date,
                                   par_scope_value        => 'GLOBAL');
         COMMIT;
         glo_b_statement_num := 504;

         LOOP
            BEGIN
               DELETE      batch_log
                     WHERE run_date < (SYSDATE - 7)
                       AND ROWNUM < 5000;
            EXCEPTION
               WHEN OTHERS
               THEN
                  EXIT;
            END;

            EXIT WHEN SQL%NOTFOUND;
            COMMIT;
         END LOOP;
      END IF;

      glo_b_statement_num := 505;

      LOOP
         loc_sleep_time :=
               (  86400
                * (  TO_DATE (par_run_date || ' 23:59:59',
                              'DD-MON-YYYY HH24:MI:SS')
                   - SYSDATE))
             + 60;

         IF (loc_sleep_time > 600)
         THEN
            pack_exception.proc_recdebug (   'Sleeping for '
                                          || '10 minutes . Current time =<'
                                          || TO_CHAR (SYSDATE,
                                                      'DD-MON-YYYY HH24:MI:SS')
                                          || '>');
            DBMS_LOCK.sleep (600);
         ELSIF (loc_sleep_time > 0)
         THEN
            pack_exception.proc_recdebug (   'Sleeping for '
                                          || loc_sleep_time
                                          || ' seconds . Current time =<'
                                          || TO_CHAR (SYSDATE,
                                                      'DD-MON-YYYY HH24:MI:SS')
                                          || '>');
            DBMS_LOCK.sleep (loc_sleep_time);
            EXIT;
         ELSE
            EXIT;
         END IF;
      END LOOP;

      glo_b_statement_num := 507;
      DBMS_LOCK.sleep (NVL (par_flag, 0) * 60);
      COMMIT;
      pack_batch_util.proc_batch_endup ('SUCCESS');
      RETURN 0;
   EXCEPTION
      WHEN OTHERS
      THEN
         ROLLBACK;
         pack_exception.proc_notpropagate;
         pack_exception.proc_recngo (par_package_name        => glo_b_package_name,
                                     par_procedure_name      => loc_procname,
                                     par_statement_num       => glo_b_statement_num,
                                     par_error_desc          =>    SQLERRM
                                                                || loc_err_msg);
         pack_batch_util.proc_batch_endup ('FAILURE');
         RETURN 1;
   END func_daily000;

   FUNCTION func_get_run_command (par_batch_name IN VARCHAR2)
      RETURN VARCHAR2
   IS
      loc_run_command   VARCHAR2 (1000);
   BEGIN
      SELECT run_command
        INTO loc_run_command
        FROM tmp_run_batch
       WHERE batch_name = par_batch_name;

      RETURN loc_run_command;
   EXCEPTION
      WHEN OTHERS
      THEN
         RETURN '0';
   END func_get_run_command;

   PROCEDURE proc_send_mail_group (
      par_recipient   IN   VARCHAR2,
      par_subj        IN   VARCHAR2,
      par_body        IN   VARCHAR2)
   IS
      i                   NUMBER              := 0;
      j                   NUMBER              := 0;
      k                   NUMBER              := 0;
      loc_email_add       VARCHAR2 (100);
      loc_length          NUMBER              := 0;
      loc_crlf            VARCHAR2 (2)        := CHR (13) || CHR (10);
      loc_mesg            VARCHAR2 (4000);
      loc_mail_conn       UTL_SMTP.connection;
      loc_cc_recipient    VARCHAR2 (50)       DEFAULT '';
      loc_bcc_recipient   VARCHAR2 (50)       DEFAULT '';
      loc_sender          VARCHAR2 (120);
      loc_user            VARCHAR2 (8);
      loc_first_name      VARCHAR2 (30);
      loc_last_name       VARCHAR2 (60);
      loc_sender_email    VARCHAR2 (120)      := NULL;
      loc_send_mail       VARCHAR2 (1);
      loc_user_override   VARCHAR2 (1);
      loc_audit_yn        VARCHAR2 (1);
      loc_par_recipient   VARCHAR2 (120);
   BEGIN
      SELECT VALUE
        INTO loc_send_mail
        FROM envvar
       WHERE variable_name = 'SEND_MAIL';

      IF loc_send_mail = 'N'
      THEN
         DBMS_OUTPUT.put_line ('PROC_SEND_MAIL is Turned Off');
      ELSE
         SELECT VALUE
           INTO loc_user_override
           FROM codis.envvar
          WHERE variable_name = 'SEND_MAIL_TEST';

         SELECT VALUE
           INTO loc_audit_yn
           FROM codis.envvar
          WHERE variable_name = 'SEND_MAIL_AUD';

         SELECT UPPER (SUBSTR (USER, 5))
           INTO loc_user
           FROM DUAL;

         SELECT INITCAP (forename)
           INTO loc_first_name
           FROM mail_addr_lookup
          WHERE stf_id = loc_user;

         SELECT INITCAP (NAME)
           INTO loc_last_name
           FROM mail_addr_lookup
          WHERE stf_id = loc_user;

         IF loc_user_override = 'Y'
         THEN
            loc_par_recipient := 'john.sawyer@churchill.com';
         ELSE
            loc_par_recipient := par_recipient;
         END IF;

         loc_sender_email := CONCAT (loc_first_name, '.');
         loc_sender_email := CONCAT (loc_sender_email, loc_last_name);
         loc_sender_email := CONCAT (loc_sender_email, '@churchill.com');
         loc_sender := loc_sender_email;
         loc_mail_conn := UTL_SMTP.open_connection ('mailhost', 25);
         UTL_SMTP.helo (loc_mail_conn, 'mailhost');
         UTL_SMTP.mail (loc_mail_conn, loc_sender);

         BEGIN
            loc_length := LENGTH (loc_par_recipient);
            j := 1;
            i := 1;

            FOR i IN 1 .. loc_length
            LOOP
               k := INSTR (loc_par_recipient, ' ', 1, i);

               IF (j = k)
               THEN
                  loc_email_add := NULL;
               ELSIF k = 0
               THEN
                  loc_email_add :=
                     LTRIM (RTRIM (SUBSTR (loc_par_recipient, j, loc_length)));
               ELSE
                  loc_email_add :=
                         LTRIM (RTRIM (SUBSTR (loc_par_recipient, j, k - j)));
               END IF;

               j := k + 1;
               UTL_SMTP.rcpt (loc_mail_conn, loc_email_add);
               loc_mesg :=
                      'Date: '
                   || TO_CHAR (SYSDATE, 'dd Mon yy hh24:mi:ss')
                   || loc_crlf
                   || 'From: '
                   || loc_sender
                   || loc_crlf
                   || 'To: '
                   || loc_par_recipient
                   || loc_crlf
                   || 'Cc: '
                   || loc_cc_recipient
                   || loc_crlf
                   || 'Bcc: '
                   || loc_bcc_recipient
                   || loc_crlf
                   || 'Subject: '
                   || par_subj
                   || loc_crlf;
               loc_mesg := loc_mesg || '' || loc_crlf || par_body;
               EXIT WHEN k = 0;
            END LOOP;
         END;

         UTL_SMTP.DATA (loc_mail_conn, loc_mesg);
         UTL_SMTP.quit (loc_mail_conn);

         IF loc_audit_yn = 'Y'
         THEN
            INSERT INTO dbmail.send_mail_audit
                        (send_date,
                         sender,
                         recipient,
                         copied,
                         blind_copied,
                         subject)
                 VALUES (SYSDATE,
                         loc_sender,
                         loc_par_recipient,
                         loc_cc_recipient,
                         loc_bcc_recipient,
                         par_subj);
         END IF;
      END IF;
   EXCEPTION
      WHEN UTL_SMTP.transient_error OR UTL_SMTP.permanent_error
      THEN
         UTL_SMTP.quit (loc_mail_conn);
         DBMS_OUTPUT.put_line (SQLERRM);
         RAISE;
      WHEN OTHERS
      THEN
         DBMS_OUTPUT.put_line (SQLERRM);
         RAISE;
   END proc_send_mail_group;

   FUNCTION func_get_loader_file_name (
      par_batch_name   IN   VARCHAR2,
      par_run_day      IN   VARCHAR2)
      RETURN VARCHAR2
   IS
      loc_file_name    VARCHAR2 (200);
      loc_file_names   VARCHAR2 (4000);
      loc_flag         NUMBER          := 1;

      CURSOR cur_file_names
      IS
         SELECT   file_name
             FROM (SELECT REPLACE (file_name, '${DAY}', par_run_day)
                                                                    file_name,
                          file_seq
                     FROM tmp_run_loader
                    WHERE UPPER (batch_name) = UPPER (par_batch_name)
                      AND loc_flag = 1
                   UNION ALL
                   SELECT REPLACE (file_name, '${DAY}', par_run_day)
                                                                    file_name,
                          file_seq
                     FROM tmp_run_loader
                    WHERE UPPER (batch_name) = UPPER (par_batch_name)
                      AND UPPER (file_name) = 'AVG_${DAY}_VDN'
                      AND loc_flag = 2
                   UNION ALL
                   SELECT REPLACE (file_name, '${DAY}', par_run_day)
                                                                    file_name,
                          file_seq
                     FROM tmp_run_loader
                    WHERE UPPER (batch_name) = UPPER (par_batch_name)
                      AND UPPER (file_name) != 'AVG_${DAY}_VDN'
                      AND loc_flag = 3)
         ORDER BY file_seq;
   BEGIN
      glo_b_statement_num := 601;

      IF UPPER (par_batch_name) = 'EISU242'
      THEN
         IF UPPER (par_run_day) = 'SATURDAY'
         THEN
            loc_flag := '3';
         ELSE
            loc_flag := '2';
         END IF;
      END IF;

      glo_b_statement_num := 605;
      OPEN cur_file_names;

      LOOP
         FETCH cur_file_names INTO loc_file_name;
         EXIT WHEN cur_file_names%NOTFOUND;

         IF cur_file_names%ROWCOUNT = 1
         THEN
            loc_file_names := loc_file_name;
         ELSE
            loc_file_names := loc_file_names || ' ' || loc_file_name;
         END IF;
      END LOOP;

      glo_b_statement_num := 610;

      IF cur_file_names%ROWCOUNT = 0
      THEN
         pack_exception.proc_reclog (   'No Data file name found for batch <'
                                     || par_batch_name
                                     || '>');
      END IF;

      CLOSE cur_file_names;
      glo_b_statement_num := 615;
      RETURN (NVL (loc_file_names, 0));
   EXCEPTION
      WHEN OTHERS
      THEN
         pack_exception.proc_recngo (par_package_name        => glo_b_package_name,
                                     par_procedure_name      => 'func_get_file_name',
                                     par_statement_num       => glo_b_statement_num,
                                     par_error_desc          => SQLERRM,
                                     par_message_string      =>    'par_batch_name <'
                                                                || par_batch_name
                                                                || '>, par_run_day <'
                                                                || par_run_day
                                                                || '>');
         RETURN (1);
   END func_get_loader_file_name;
   
   PROCEDURE proc_write_session_longops (
      par_op_name   IN   VARCHAR2,
      par_sofar     IN   NUMBER DEFAULT NULL)
   IS
      loc_slno               PLS_INTEGER;
      loc_rindex             PLS_INTEGER;
   BEGIN
      loc_rindex := DBMS_APPLICATION_INFO.set_session_longops_nohint;

      DBMS_APPLICATION_INFO.set_session_longops (rindex       => loc_rindex,
                                                 slno         => loc_slno,
                                                 op_name      => par_op_name,
                                                 sofar        => par_sofar);
   END proc_write_session_longops;
END pkg_batch_util;
/

SHOW error
GRANT     EXECUTE ON pkg_batch_util TO PUBLIC;

