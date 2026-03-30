set projectDir to "/Users/inverse/Applications/信息大爆炸"
set stopper to projectDir & "/stop_monitor_ui_mac.sh"

try
	do shell script "chmod +x " & quoted form of stopper
	set resultText to do shell script quoted form of stopper
on error errMsg number errNum
	display alert "信息大爆炸停止失败" message errMsg as critical
	return
end try

if resultText is "stopped" then
	display notification "监控 UI 已停止" with title "信息大爆炸"
else
	display notification resultText with title "信息大爆炸"
end if
