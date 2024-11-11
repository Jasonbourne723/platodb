@echo off
REM 无限循环
:loop
    REM 尝试执行 git push
    echo try git push
    git push
    REM 检查命令执行状态码（0 表示成功）
    IF %ERRORLEVEL% EQU 0 (
        echo Push 成功!
        exit /b 0
    ) ELSE (
        echo Push failed,retry...
        REM 等待5秒后重试
        timeout /t 2 /nobreak >nul
        GOTO loop
    )
