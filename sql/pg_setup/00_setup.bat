@ECHO OFF
CHCP 932 >NUL
SETLOCAL EnableDelayedExpansion

cd /d %~dp0

REM Load configuration from config file
IF EXIST config.bat (
    ECHO Loading config.bat...
    CALL config.bat
) ELSE (
    ECHO Using default configuration...
    SET dbhost=127.0.0.1
    SET dbname=postal_code_db
    SET dbuser=postgres
)

ECHO Configuration: host=!dbhost! db=!dbname! user=!dbuser!

ECHO PostgreSQL Database Setup
ECHO Creating database and tables...
ECHO;
ECHO To enable password-less connection, create:
ECHO %APPDATA%\postgresql\pgpass.conf
ECHO With content: !dbhost!:5432:*:!dbuser!:your_password
ECHO (Replace 'your_password' with actual password)
ECHO;

REM Check if psql is available
psql -V >NUL 2>&1
IF ERRORLEVEL 1 (
    ECHO Error: psql command not found. Please install PostgreSQL client.
    PAUSE
    EXIT /B 1
)

ECHO PostgreSQL client connection verified
ECHO;

ECHO Database: !dbname!
ECHO Host: !dbhost!
ECHO User: !dbuser!
ECHO;
ECHO If database exists, it will be renamed with timestamp.
ECHO To drop old databases, use following SQL:
ECHO SELECT 'DROP DATABASE ' ^|^| datname ^|^| ';' as command FROM pg_database WHERE datname LIKE 'postal_code_db%%';

PAUSE

ECHO Renaming existing database if exists...
SET now=%time: =0%
SET yyyymmddhhmmss=%date:/=%%now:~0,2%%now:~3,2%%now:~6,2%
SET sql=ALTER DATABASE !dbname! RENAME TO !dbname!_%yyyymmddhhmmss%;
psql -h "!dbhost!" -d "postgres" -U "!dbuser!" -w -c "!sql!"
IF ERRORLEVEL 1 (
    ECHO Warning: Failed to rename existing database ^(may not exist^)
)
ECHO;

ECHO Creating database and tables...
PAUSE

ECHO Creating database !dbname!...
SET sql2=CREATE DATABASE !dbname!;
psql -h "!dbhost!" -d "postgres" -U "!dbuser!" -w -c "!sql2!"
IF ERRORLEVEL 1 (
    ECHO Error: Failed to create database.
    PAUSE
    EXIT /B 1
)
ECHO Database creation completed

CALL 01_00_create_tables.bat
IF ERRORLEVEL 1 (
    ECHO Error: Table creation failed.
    PAUSE
    EXIT /B 1
)

CALL 02_00_create_indexes.bat
IF ERRORLEVEL 1 (
    ECHO Error: Index creation failed.
    PAUSE
    EXIT /B 1
)

REM CALL 03_00_create_triggers.bat (file does not exist - commented out)

ECHO;
ECHO Setup completed successfully!
ECHO For extensions, manually run ext_create_extensions.sql after configuring postgresql.conf
PAUSE