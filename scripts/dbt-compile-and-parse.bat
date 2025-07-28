@echo off
REM Helper script for Windows to compile metrics and run dbt commands
REM Usage: dbt-compile-and-parse.bat [dbt command and args]
REM Example: dbt-compile-and-parse.bat parse
REM Example: dbt-compile-and-parse.bat run

REM Get the directory where this script is located
set SCRIPT_DIR=%~dp0

REM Get the dbt project root (assuming standard dbt package structure)
pushd %SCRIPT_DIR%..\..\..\..\
set DBT_PROJECT_ROOT=%CD%
popd

echo Compiling metrics-first files...
echo    Working directory: %DBT_PROJECT_ROOT%

REM Run the compiler from the project root
pushd %DBT_PROJECT_ROOT%
python "%SCRIPT_DIR%compile_metrics.py" %*

REM Check if compilation was successful
if %ERRORLEVEL% EQU 0 (
    echo Metrics compilation successful
    
    REM If dbt command was provided, run it
    if not "%~1"=="" (
        echo.
        echo Running: dbt %*
        dbt %*
    ) else (
        echo.
        echo No dbt command specified. Run any dbt command now.
        echo    Example: dbt parse
    )
) else (
    echo Metrics compilation failed
    popd
    exit /b 1
)

popd