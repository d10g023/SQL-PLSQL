CREATE OR REPLACE PROCEDURE P_CREATE_TOOLS_LOG(SCHEMA VARCHAR, NAME VARCHAR, CSV_FILES TEXT) AS $$
    -- Procedure to create the stack of objects that can help to analyze the postgres csvlog file
    -- To use this stack of objects is needed have log_min_duration_statement >= 0 and log_destination = 'csvlog' parameter configured on postgresql.conf
    -- This procedure need 3 parameters:
    --      SCHEMA    -> Schema where the objects will be created
    --      NAME      -> The name used to identify the set of objects created
    --      CSV_FILES -> The file or files will be loaded to the table log. Is possible load more then 1 file using comma. ex: 'postgres_log_1.csv,postgres_log_2.csv'
    --
DECLARE
    SQL VARCHAR;
BEGIN

    -- Procedure used to load cvslog file from postgres log
    -- This procedure need 1 parameter:
    --      CSV_FILES -> The file or files will be loaded to the table log. Is possible load more then 1 file using comma. ex: 'postgres_log_1.csv,postgres_log_2.csv'
    -- 
    SQL = 'CREATE OR REPLACE PROCEDURE '|| SCHEMA || '.P_'||NAME||'_LOAD_FILES(CSV_FILES VARCHAR)
    AS $'||''||'$

    DECLARE
        SQL VARCHAR;
        FILE_NAME VARCHAR;
    BEGIN
        FOREACH FILE_NAME IN ARRAY STRING_TO_ARRAY(CSV_FILES,'','')
        LOOP
            SQL = ''COPY '|| SCHEMA || '.T_' || NAME || '(LOG_TIME,USER_NAME,DATABASE_NAME,PROCESS_ID,CONNECTION_FROM,SESSION_ID,SESSION_LINE_NUM,COMMAND_TAG,SESSION_START_TIME,VIRTUAL_TRANSACTION_ID,TRANSACTION_ID,ERROR_SEVERITY,SQL_STATE_CODE,MESSAGE,DETAIL,HINT,INTERNAL_QUERY,INTERNAL_QUERY_POS,CONTEXT,QUERY,QUERY_POS,LOCATION,APPLICATION_NAME) FROM '''''' || FILE_NAME || '''''' WITH CSV;'';
            EXECUTE SQL;
            SQL = ''UPDATE '|| SCHEMA || '.T_' || NAME || ' SET MESSAGE_QUERY = SUBSTR(MESSAGE,	POSITION('''':'''' IN MESSAGE) + 1, LENGTH(MESSAGE)) WHERE MESSAGE LIKE ''''duration: %'''' AND ARRAY_LENGTH(STRING_TO_ARRAY(MESSAGE, '''':''''), 1) - 1 >= 2;'';
            EXECUTE SQL;
            SQL = ''UPDATE '|| SCHEMA || '.T_' || NAME || ' SET	MESSAGE_QUERY = SUBSTR(MESSAGE_QUERY, POSITION('''':'''' IN MESSAGE_QUERY) + 1, LENGTH(MESSAGE_QUERY)) WHERE MESSAGE_QUERY NOTNULL;'';
            EXECUTE SQL;
            SQL = ''UPDATE '|| SCHEMA || '.T_' || NAME || ' SET	MESSAGE_QUERY =  SUBSTR(MESSAGE,POSITION('''':'''' IN MESSAGE) + 1,LENGTH(MESSAGE)) WHERE MESSAGE NOT LIKE ''''duration: %'''' AND ARRAY_LENGTH(STRING_TO_ARRAY(MESSAGE, '''':''''), 1) - 1 >= 1;'';
            EXECUTE SQL;
            SQL = ''UPDATE '|| SCHEMA || '.T_' || NAME || ' SET HASH_MESSAGE = ENCODE(SHA256(MESSAGE_QUERY::BYTEA), ''''HEX'''') WHERE HASH_MESSAGE IS NULL;'';
            EXECUTE SQL;
            SQL = ''UPDATE '|| SCHEMA || '.T_' || NAME || ' SET DURATION = SPLIT_PART(MESSAGE,'''' '''',2)::FLOAT WHERE MESSAGE LIKE ''''duration: %'''';'';
            EXECUTE SQL;
        END LOOP;
    END; $'||''||'$ 
    LANGUAGE PLPGSQL;';
    EXECUTE SQL;

    -- Table used to load csvlog file 
    --
    SQL = 'CREATE TABLE '|| SCHEMA || '.T_' || NAME || ' (
        HASH_MESSAGE VARCHAR(128),
        LOG_TIME TIMESTAMP(3) WITH TIME ZONE,
        USER_NAME TEXT,
        DATABASE_NAME TEXT,
        PROCESS_ID INTEGER,
        CONNECTION_FROM TEXT,
        SESSION_ID TEXT,
        SESSION_LINE_NUM BIGINT,
        COMMAND_TAG TEXT,
        SESSION_START_TIME TIMESTAMP WITH TIME ZONE,
        VIRTUAL_TRANSACTION_ID TEXT,
        TRANSACTION_ID BIGINT,
        ERROR_SEVERITY TEXT,
        SQL_STATE_CODE TEXT,
        MESSAGE TEXT,
        DURATION FLOAT,
        MESSAGE_QUERY TEXT,
        DETAIL TEXT,
        HINT TEXT,
        INTERNAL_QUERY TEXT,
        INTERNAL_QUERY_POS INTEGER,
        CONTEXT TEXT,
        QUERY TEXT,
        QUERY_POS INTEGER,
        LOCATION TEXT,
        APPLICATION_NAME TEXT,
        PRIMARY KEY (SESSION_ID, SESSION_LINE_NUM)
    );';
    EXECUTE SQL;

    -- Call of LOAD_FILES procedure to load the data from the csvlog file to log table 
    --
    SQL = 'CALL '|| SCHEMA || '.P_'||NAME||'_LOAD_FILES('''|| CSV_FILES ||''')';
    EXECUTE SQL;

    -- Function used to know the top ran queries by the log loaded
    -- Return columns:
    --      NUM_EXEC      -> The number of executions with the same hash, database and query
    --      HASH_MESSAGE  -> Hash of MESSAGE_QUERY column. Used to identify the same querys
    --      DATABASE_NAME -> The database where the query was ran by csvlog file
    --      MESSAGE_QUERY -> The text of query
    -- 
    SQL = 'CREATE OR REPLACE FUNCTION '|| SCHEMA || '.F_'||NAME||'_NUM_MESSAGE (L INTEGER) 
            RETURNS TABLE (
                NUM_EXEC BIGINT,
                HASH_MESSAGE VARCHAR(128),
                DATABASE_NAME TEXT,
                MESSAGE_QUERY TEXT
        )
        AS $'||''||'$
        BEGIN
            RETURN QUERY 
            SELECT
                COUNT(1) AS NUM_EXEC,
                T.HASH_MESSAGE,
                T.DATABASE_NAME,
                T.MESSAGE_QUERY
            FROM
                '|| SCHEMA ||'.T_'|| NAME ||' AS T
            WHERE
                T.COMMAND_TAG NOTNULL 
                AND T.COMMAND_TAG != ''''
                AND T.MESSAGE_QUERY NOTNULL
            GROUP BY
                T.DATABASE_NAME, T.HASH_MESSAGE, T.MESSAGE_QUERY
            ORDER BY NUM_EXEC DESC 
            LIMIT L;
        END; $'||''||'$ 
        LANGUAGE PLPGSQL;';
    EXECUTE SQL;

    -- Function to get the parameters of querys. The parameter needed is the hash on hash_message column 
    -- Return columns:
    --      HASH_MESSAGE -> Hash of MESSAGE_QUERY column. Used to identify the same querys
    --      LOG_TIME     -> Time when the query was ran 
    --      DETAIL       -> Values of bind variables 
    --
    SQL = 'CREATE OR REPLACE FUNCTION '|| SCHEMA || '.F_'||NAME||'_GET_QUERY_PARAMETERS (HASH VARCHAR) 
        RETURNS TABLE (
            HASH_MESSAGE VARCHAR(128),
            LOG_TIME TIMESTAMP(3) WITH TIME ZONE,
            DETAIL TEXT
    )
    AS $'||''||'$
    BEGIN
        RETURN QUERY 
        SELECT
            T.HASH_MESSAGE,
            T.LOG_TIME,
            T.DETAIL
        FROM
            '|| SCHEMA ||'.T_'|| NAME ||' AS T
        WHERE
            T.COMMAND_TAG NOTNULL 
            AND T.COMMAND_TAG != ''''
            AND T.HASH_MESSAGE = HASH;
    END; $'||''||'$ 
    LANGUAGE PLPGSQL;';
    EXECUTE SQL;

    COMMIT;

END; $$ 
LANGUAGE PLPGSQL;
