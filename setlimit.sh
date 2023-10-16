limit_dir="/home/yuhang/NovaLSM/scripts/bootstrap"

sudo mv /etc/systemd/user.conf /etc/systemd/user.conf.bat
sudo mv /etc/systemd/system.conf /etc/systemd/system.conf.bat
sudo mv /etc/security/limits.conf /etc/security/limits.conf.bat

sudo cp /home/yuhang/NovaLSM/scripts/bootstrap/ulimit.conf /etc/systemd/user.conf
sudo cp /home/yuhang/NovaLSM/scripts/bootstrap/sys_ulimit.conf /etc/systemd/system.conf
sudo cp /home/yuhang/NovaLSM/scripts/bootstrap/limit.conf /etc/security/limits.conf

# sudo reboot
