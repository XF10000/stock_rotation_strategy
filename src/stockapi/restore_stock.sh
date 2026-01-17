HOME_DIR=/data/stockapp
application_sub_dir=/src/stockapi
application_name='stockWebAPI'
application_app='application'
application_port='8888'
application_num='3'
#application_gunicorn=/usr/local/bin/gunicorn
#application_gunicorn=/usr/bin/gunicorn
application_gunicorn=/data/userbin/python3/bin/gunicorn
echo application=$application_name
echo
echo ps aux|grep -E ${application_name}|grep -v grep|awk '{print $2}'|xargs kill -9
ps aux|grep -E ${application_name}|grep -v grep|awk '{print $2}'|xargs kill -9
echo
cd ${HOME_DIR}${application_sub_dir}
echo gunicorn $application_name
$application_gunicorn --reload -w $application_num -b 127.0.0.1:$application_port $application_name:$application_app &
#gunicorn -b 127.0.0.1:$application_port $application_name:$application_app &
