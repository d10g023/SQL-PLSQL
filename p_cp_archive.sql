-- This script was created to have a history and help manager of the archive logs 
-- Is recommended run this script on default database postgres
--
CREATE SCHEMA DBA;

-- The table used to configure the procedure
--
CREATE TABLE dba.t_proc_conf (
    name text PRIMARY KEY,
    value text
);

-- Is necessary 2 records or more. First record is the cp command, the command shell used to copy archive. 
-- Is necessery the command have {0} (path pg_wal) and {1} (cp_path present on table). Ex: cp {0} {1}, rsync {0} {1}
-- the others records are cp_pathn where they are used to copy the archive to multiple paths. Ex:  cp_path1 -> /tmp
--
INSERT INTO dba.t_proc_conf VALUES ('cp_command', 'cp {0} {1}');
INSERT INTO dba.t_proc_conf VALUES ('cp_path1', '/tmp');

-- The table used to log the archives timestamp, name and if get error or not on the copy 
--
CREATE TABLE dba.t_archive_log (
    ARCHIVE_NAME TEXT,
    ARCHIVE_PATH TEXT,
    ARCHIVE_TIME TIMESTAMP WITHOUT TIME ZONE,
    RETURN_CODE CHARACTER VARYING(100)
);

-- The procedure where the magic it happens.

CREATE OR REPLACE PROCEDURE dba.p_cp_archive(a text) AS 
    $BODY$
        import os
        plan = plpy.prepare("insert into dba.t_archive_log values($1,$2,current_timestamp,$3)", ["text", "text", "text"])
        c = plpy.execute("SELECT value FROM dba.t_proc_conf where name = 'cp_command'")
        p = plpy.execute("SELECT value FROM dba.t_proc_conf where name like 'cp_path%'")
        for pths in p:
            e = os.system(c[0]['value'].format('pg_wal/' + a, pths['value'] + '/' + a))
            if e == 0:
                v = 'Success'
            else:
                v = 'Error: Check postgres log file'
            plpy.execute(plan, [a, pths['value'], v])
            plpy.commit()
            if e != 0:
                plpy.error(v)
    $BODY$
LANGUAGE plpython3u;

-- To use this procedure is needed configure the archive_command on postgresql.conf to use the following command:
-- archive_command = 'psql -c "call dba.p_cp_archive(\'%f\')"'
