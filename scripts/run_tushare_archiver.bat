@echo off
:: TushareArchiverPublic Automation Script for Windows
:: One-click data update tool

echo ============================================
echo TushareArchiverPublic Data Update Tool
echo ============================================
echo.

:: Display start time
echo Start time: %date% %time%
echo.

:: Activate conda environment
echo [1/3] Activating conda environment (tushare)...
call conda activate tushare
if %errorlevel% neq 0 (
    echo ERROR: Cannot activate conda environment 'tushare'
    echo Please ensure:
    echo   1. Anaconda/Miniconda is installed
    echo   2. Created conda environment named 'tushare'
    echo   3. conda command is in PATH
    echo Note: You can change the environment name by editing this script
    pause
    exit /b 1
)
echo Conda environment activated successfully
echo.

:: Change to project directory
echo [2/3] Changing to project directory...
cd /d "PATH_TO_YOUR_PROJECT_ROOT"
if %errorlevel% neq 0 (
    echo ERROR: Cannot change to project directory
    echo Current directory: %cd%
    echo Please replace PATH_TO_YOUR_PROJECT_ROOT with your actual project path
    pause
    exit /b 1
)
echo Current directory: %cd%
echo.

:: Execute data update
echo [3/3] Starting data update...
echo ============================================
python main.py

:: Save exit code
set main_exit_code=%errorlevel%

echo.
echo ============================================
echo Execution completed
echo End time: %date% %time%

:: Show result based on exit code
if %main_exit_code% equ 0 (
    echo Status: SUCCESS
    echo All data modules updated successfully!
) else (
    echo Status: FAILED (Exit code: %main_exit_code%)
    echo Some modules failed to update, please check log files
)

echo.
echo Press any key to exit...
pause >nul 