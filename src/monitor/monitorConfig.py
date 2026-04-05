_VERSION = "20260405"

currWorkMode="monitor"

monitorWorkDir = r"/data/stockapp/src/monitor"

monitorFileStatusFileName = "tempMonitorFileName.json"

processData = [
#common part
#{"key":"redis-server","cmd":"sh /data/redis/restore > /dev/null ","param":":16379"}, #redis service
#{"key":"nginx","cmd":"/usr/local/nginx/nginx > /dev/null ","param":""}, #nginx service
#application
    {
        "key":"accountApp:application",
        "cmd":"cd /data/accountService/src/accountService; ./restore_acc.sh ", 
        "param":"",
        "file":"/data/accountService/src/accountService/accountAppPost.py",
        "fileList":["/data/accountService/src/config/accountsettings.py",
                    "/data/accountService/src/config/local_settings.py",
                    "/data/accountService/src/config/accountMysqlSettings.py",
                    "/data/accountService/src/config/accountRedisSettings.py",],
        # "num":3, #不检查数量就不要填, 或者填0
    },
    {
        "key":"stockWebAPI:application",
        "cmd":"cd /data/stockapp/src/stockapi; ./restore_stock.sh ", 
        "param":"",
        "file":"/data/stockapp/src/stockapi/stockWebAPIPost.py",
        "fileList":["/data/stockapp/src/config/basicSettings.py",
                    "/data/stockapp/src/config/local_settings.py",
                    "/data/stockapp/src/config/redisSettings.py",
                    "/data/stockapp/src/config/mysqlSettings.py",
                    "/data/stockapp/src/common/globalDefinition.py",
                    "/data/stockapp/src/common/funcCommon.py",],
        # "num":3, #不检查数量就不要填, 或者填0
    },
    {
        "key":"ylwzRecvFiles:application",
        "cmd":"cd /data/stockapp/src/stockapi; ./restore_file.sh ", 
        "param":"",
        "file":"/data/stockapp/src/stockapi/ylwzRecvFiles.py",
        "fileList":["/data/stockapp/src/config/basicSettings.py",
                    "/data/stockapp/src/config/local_settings.py",
                    "/data/stockapp/src/common/globalDefinition.py",
                    "/data/stockapp/src/config/redisSettings.py",
                    "/data/stockapp/src/common/funcCommon.py",],
        # "num":3, #不检查数量就不要填, 或者填0
    },
    {
        "key":"transferStockMysql.py",
        "cmd":"cd /data/stockapp/src/processor; ./restore_trans.sh ", 
        "param":"",
        "file":"/data/stockapp/src/processor/transferStockMysql.py",
        "fileList":["/data/stockapp/src/config/basicSettings.py",
                    "/data/stockapp/src/config/local_settings.py",
                    "/data/stockapp/src/common/globalDefinition.py",
                    "/data/stockapp/src/common/funcCommon.py",
                    "/data/stockapp/src/config/redisSettings.py",
                    "/data/stockapp/src/config/mysqlSettings.py",],
        # "num":3, #不检查数量就不要填, 或者填0
    },
]

existProcessKeys = [
]

serviceMonitorData = [

]
