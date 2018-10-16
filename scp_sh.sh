set timeout 3000
set server_host "12.1.10.168"
set server_user "root"
set password "cxxt@123"

set local_root "./"
set server_root "~/project/gs_spark/"

spawn scp -r $local_root $server_user@$server_host:$server_root

expect {
    "*password:" {
        send "$password\r"
        exp_continue
    }
    "*?" {
        send "y\r"
    }
}

expect eof

