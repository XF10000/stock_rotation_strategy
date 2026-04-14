echo restore_monitor.sh
HOME_DIR='/data/stockapp'
application_name='monitor.py'
cd ${HOME_DIR}/src/monitor
ps aux|grep -E ${application_name} |grep -v grep|awk '{print $2}'|xargs kill
python3 ${application_name}
