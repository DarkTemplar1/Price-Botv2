@echo off
REM ========================================================
REM        BUILD PriceBot.exe
REM ========================================================

set APP_NAME=PriceBot
set MAIN_SCRIPT=selektor_csv.py

echo ========================================
echo        BUILDING %APP_NAME%.exe
echo ========================================

REM ---- Ścieżka do Pythona z bieżącego środowiska ----
set PYTHON_EXE=%~dp0.venv\Scripts\python.exe
echo [INFO] Używany Python: %PYTHON_EXE%

REM ---- Sprawdzenie PyInstaller ----
echo [INFO] Sprawdzanie PyInstaller...
"%PYTHON_EXE%" -m pip install pyinstaller >nul 2>&1

REM ---- Budowanie EXE ----
echo [INFO] Tworzenie EXE. Log: build_debug.log
"%PYTHON_EXE%" -m PyInstaller ^
 --onefile ^
 --noconfirm ^
 --windowed ^
 --name %APP_NAME% ^
 "%MAIN_SCRIPT%" > build_debug.log 2>&1

echo ---------------------------------------------------------------
echo [OK] GOTOWE
echo    dist\%APP_NAME%.exe
echo ---------------------------------------------------------------

pause