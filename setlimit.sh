limit_dir="$REMOTE_HOME/NovaLSM/scripts/bootstrap"

sudo mv /etc/systemd/user.conf /etc/systemd/user.conf.bat
sudo mv /etc/systemd/system.conf /etc/systemd/system.conf.bat
sudo mv /etc/security/limits.conf /etc/security/limits.conf.bat

sudo cp $limit_dir/ulimit.conf /etc/systemd/user.conf
sudo cp $limit_dir/sys_ulimit.conf /etc/systemd/system.conf
sudo cp $limit_dir/limit.conf /etc/security/limits.conf

# sudo reboot
