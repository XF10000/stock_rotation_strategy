application_name='ylwzRecvFiles'
application_app='application'
application_port='8005'
application_num='2'
#application_gunicorn=/usr/local/bin/gunicorn
#application_gunicorn=/usr/bin/gunicorn
application_gunicorn=/data/userbin/python3/bin/gunicorn
echo application=$application_name
echo
echo ps aux|grep -E ${application_name}|grep -v grep|awk '{print $2}'|xargs kill -9
ps aux|grep -E ${application_name}|grep -v grep|awk '{print $2}'|xargs kill -9
echo
echo gunicorn $application_name
$application_gunicorn --reload -w $application_num -b 127.0.0.1:$application_port $application_name:$application_app &
#gunicorn -b 127.0.0.1:$application_port $application_name:$application_app &
