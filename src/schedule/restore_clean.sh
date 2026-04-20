WORKING_DIR=/data/stockapp/src/schedule
#pythonApp=/usr/bin/python3
pythonApp=/data/userbin/python3/bin/python3
application=dataClean
cd $WORKING_DIR
${pythonApp} ${application}.py
