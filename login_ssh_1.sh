set timeout 20
set ip "12.1.10.168"
set user "root"
set password "cxxt@123"

spawn ssh "$user\@$ip"

expect "$user@$ip's password:"
send "$password\r"

interact
