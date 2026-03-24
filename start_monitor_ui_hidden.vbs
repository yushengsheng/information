Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")

projectDir = fso.GetParentFolderName(WScript.ScriptFullName)
killCmd = "powershell -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File """ & projectDir & "\stop_monitor_ui.ps1"""
pythonCmd = "pythonw app.py"

shell.CurrentDirectory = projectDir
shell.Run killCmd, 0, True
shell.Run pythonCmd, 0, False
