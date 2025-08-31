@ECHO OFF
CHCP 932 >NUL
SETLOCAL EnableDelayedExpansion

cd /d %~dp0

REM Load configuration from config file
IF EXIST config.bat (
    CALL config.bat
) ELSE (
    SET dbhost=127.0.0.1
    SET dbname=postal_code_db
    SET dbuser=postgres
)

ECHO Creating tables...

ECHO Creating schema...
psql -h "!dbhost!" -d "!dbname!" -U "!dbuser!" -w -f 01_01_create_schema_sjis.sql
IF ERRORLEVEL 1 (
    ECHO Error: Failed to create schema.
    EXIT /B 1
)

ECHO Creating extension tables...
psql -h "!dbhost!" -d "!dbname!" -U "!dbuser!" -w -f 01_02_create_ext_tables_sjis.sql
IF ERRORLEVEL 1 (
    ECHO Error: Failed to create extension tables.
    EXIT /B 1
)

ECHO Table creation completed
ECHO;
