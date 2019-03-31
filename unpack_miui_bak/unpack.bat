@echo off
color b
title 解包小米备份数据  By：菲菲博客 - www.feifeiboke.com
echo.
set bakFile=%1
if defined bakFile (goto javas) else set /p bakFile=请拖入修改后的Bak文件：
:javas
python miuibak_to_abe.py %bakFile%
java -jar "%~dp0\abe.jar" unpack tmp.jar %bakFile%.tar
del tmp.jar
echo.
echo 操作结束...
pause>nul