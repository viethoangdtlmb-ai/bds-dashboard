@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM ============================================================
REM  BDS DASHBOARD - AUTO CRAWL & PUSH (Chay moi ngay 21:00)
REM  Tac gia: Asset Architect System
REM ============================================================

set "PROJECT_DIR=d:\1. BDS\AI-Assistant\bds-dashboard"
set "PYTHON=C:\Users\Admin\AppData\Local\Programs\Python\Python312\python.exe"
set "LOG_DIR=%PROJECT_DIR%\logs"
set "TIMESTAMP=%date:~6,4%-%date:~3,2%-%date:~0,2%_%time:~0,2%%time:~3,2%"
set "TIMESTAMP=%TIMESTAMP: =0%"
set "LOG_FILE=%LOG_DIR%\crawl_%TIMESTAMP%.log"

REM Tao thu muc log neu chua co
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

echo ============================================================ >> "%LOG_FILE%" 2>&1
echo [%date% %time%] BAT DAU CRAWL BDS DASHBOARD >> "%LOG_FILE%" 2>&1
echo ============================================================ >> "%LOG_FILE%" 2>&1

cd /d "%PROJECT_DIR%"

REM --- BUOC 1: CRAWL DU LIEU ---
echo [%time%] Buoc 1: Crawl du lieu tu batdongsan.com.vn... >> "%LOG_FILE%" 2>&1
set "BDS_DATA_DIR=%PROJECT_DIR%\data"
"%PYTHON%" scripts/crawl_chi_so_thi_truong.py >> "%LOG_FILE%" 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo [%time%] LOI: Crawl THAT BAI! >> "%LOG_FILE%" 2>&1
    goto :END
)
echo [%time%] Crawl THANH CONG! >> "%LOG_FILE%" 2>&1

REM --- BUOC 2: TINH CHI SO PHAI SINH ---
echo [%time%] Buoc 2: Tinh chi so phai sinh... >> "%LOG_FILE%" 2>&1
"%PYTHON%" scripts/tinh_chi_so_phai_sinh.py >> "%LOG_FILE%" 2>&1
echo [%time%] Chi so phai sinh XONG! >> "%LOG_FILE%" 2>&1

REM --- BUOC 3: TAO BIEU DO DASHBOARD ---
echo [%time%] Buoc 3: Tao bieu do dashboard... >> "%LOG_FILE%" 2>&1
"%PYTHON%" scripts/xem_bieu_do.py >> "%LOG_FILE%" 2>&1
echo [%time%] Bieu do XONG! >> "%LOG_FILE%" 2>&1

REM --- BUOC 4: COPY DASHBOARD RA ROOT ---
echo [%time%] Buoc 4: Copy dashboard HTML... >> "%LOG_FILE%" 2>&1
if exist "data\BIEU_DO_THI_TRUONG.html" (
    copy /Y "data\BIEU_DO_THI_TRUONG.html" "index.html" >> "%LOG_FILE%" 2>&1
)

REM --- BUOC 5: TONG HOP TUAN ---
echo [%time%] Buoc 5: Tong hop tuan... >> "%LOG_FILE%" 2>&1
"%PYTHON%" scripts/tong_hop_tuan.py >> "%LOG_FILE%" 2>&1

REM --- BUOC 6: GIT PUSH LEN GITHUB ---
echo [%time%] Buoc 6: Git push len GitHub... >> "%LOG_FILE%" 2>&1
git add -A >> "%LOG_FILE%" 2>&1
git diff --cached --quiet
if %ERRORLEVEL% NEQ 0 (
    git commit -m "Auto update %date% %time:~0,5% VN" >> "%LOG_FILE%" 2>&1
    git pull --rebase origin main >> "%LOG_FILE%" 2>&1
    git push >> "%LOG_FILE%" 2>&1
    echo [%time%] Git push THANH CONG! >> "%LOG_FILE%" 2>&1
) else (
    echo [%time%] Khong co thay doi moi de push. >> "%LOG_FILE%" 2>&1
)

:END
echo ============================================================ >> "%LOG_FILE%" 2>&1
echo [%date% %time%] KET THUC >> "%LOG_FILE%" 2>&1
echo ============================================================ >> "%LOG_FILE%" 2>&1

REM Xoa log cu (giu 30 file gan nhat)
forfiles /P "%LOG_DIR%" /M crawl_*.log /D -30 /C "cmd /c del @path" 2>nul

endlocal
