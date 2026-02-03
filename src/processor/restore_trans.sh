HOME_DIR=/data/stockapp
applicationName=transferStockMysql
#pythonApp=/usr/bin/python3
pythonApp=/data/userbin/python3/bin/python3
echo $applicationName
cd ${HOME_DIR}/src/processor
ps aux|grep -E ${applicationName} |grep -v grep|awk '{print $2}'|xargs kill
nohup $pythonApp ${applicationName}.py > ${HOME_DIR}/log/${applicationName}.log 2>&1 &
nohup $pythonApp ${applicationName}.py > ${HOME_DIR}/log/${applicationName}.log 2>&1 &
ps -ef |grep $applicationName
