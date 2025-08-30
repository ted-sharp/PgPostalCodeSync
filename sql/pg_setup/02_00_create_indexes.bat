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

ECHO Creating indexes...
psql -h "!dbhost!" -d "!dbname!" -U "!dbuser!" -w -f 02_01_create_indexes_sjis.sql
IF ERRORLEVEL 1 (
    ECHO Error: Failed to create indexes.
    EXIT /B 1
)

ECHO Index creation completed
ECHO;
