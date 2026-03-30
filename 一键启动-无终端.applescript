set projectDir to "/Users/inverse/Applications/信息大爆炸"
set starter to projectDir & "/start_monitor_ui_mac.sh"
set appURL to "http://127.0.0.1:8765/"

try
	do shell script "chmod +x " & quoted form of starter & " " & quoted form of (projectDir & "/stop_monitor_ui_mac.sh")
	set resultText to do shell script "OPEN_BROWSER=0 " & quoted form of starter
on error errMsg number errNum
	display alert "信息大爆炸启动失败" message errMsg as critical
	return
end try

if resultText is "started" then
	open location appURL
	display notification "监控 UI 已启动" with title "信息大爆炸"
else
	display notification resultText with title "信息大爆炸"
end if
