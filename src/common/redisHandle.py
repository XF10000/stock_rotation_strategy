#! /usr/bin/env python
#encoding: utf-8
# 2019-09-01 steven

_VERSION="20230726"

import redis

def getRedisDB(host,port,db, username = "", passwd = ""):
    if passwd == "":
        redisPool = redis.ConnectionPool(host=host, port=port, db=db)
    elif username== "" and passwd != "":
        redisPool = redis.ConnectionPool(host=host, port=port, db=db, password = passwd)        
    else:
        redisPool = redis.ConnectionPool(host=host, port=port, db=db, username = username, password = passwd)        
    redisDB = redis.Redis(connection_pool=redisPool)
    return redisDB


class RedisHandle:
    def __init__(self,dbW,dbR):
        self.dbW = dbW
        self.dbR = dbR

    #save
    def save(self):
        # save 人工保存数据到硬盘
        return self.dbW.save()

    #Key相关命令
    def exists(self, key):
        # exits key 测试指定key是否存在，返回1表示存在，0不存在
        return self.dbR.exists(key)
    def delete(self, * keys):
        # del key1 key2 ....keyN  删除给定key,返回删除key的数目，0表示给定key都不存在
        return self.dbW.delete(* keys)
    def type(self, key):
        # type key 返回给定key的value类型。返回 none 表示不存在，key有string字符类型，list 链表类型 set 无序集合类型等...
        return self.dbR.type(key)
    def keys(self, pattern):
        # keys pattern 返回匹配指定模式的所有key（支持*，？，[abc ]的方式）
        return self.dbR.keys(pattern)
    def scan(self, cursor=0, match=None, count=None):
        #  Incrementally return lists of key names. Also return a cursor indicating the scan position.
        return self.dbR.scan(cursor, match, count)
    def randomkey(self):
        # randomkey 返回从当前数据库中随机选择的一个key,如果当前数据库是空的，返回空串
        return self.dbR.randomkey()
    def rename(self, oldkey, newkey):
        # rename oldkey newkey 原子的重命名一个key,如果newkey存在，将会被覆盖，返回1表示成功，0失败。失败可能是oldkey不存在或者和newkey相同
        return self.dbW.rename(oldkey, newkey)
    def renamenx(self, oldkey, newkey):
        # renamenx oldkey newkey 同上，但是如果newkey存在返回失败
        return self.dbW.dbW.renamenx(oldkey, newkey)
    def dbsize(self):
        # dbsize 返回当前数据库的key数量
        return self.dbR.dbsize()
    def expire(self, key, seconds):
        # expire key seconds 为key指定过期时间，单位是秒。返回1成功，0表示key已经设置过过期时间或者不存在
        return self.dbW.expire(key, seconds)
    def ttl(self, key):
        # ttl key 返回设置了过期时间的key的剩余过期秒数， -1表示key不存在或者没有设置过过期时间
        return self.dbR.ttl(key)
    def select(self, dbType, dbindex):
        # select db-index 通过索引选择数据库，默认连接的数据库所有是0,默认数据库数是16个。返回1表示成功，0失败
        #dbType: R从库，W主库（默认）
        if dbType == "R":
            db = dbR
        else:
            db = dbW
        return db.select(dbindex)
    def move(self, key, dbindex):
        # move key db-index  将key从当前数据库移动到指定数据库。返回1成功。0 如果key不存在，或者已经在指定数据库中
        return self.dbW.move(key, dbindex)
    def flushdb(self, key):
        # flushdb 删除当前数据库中所有key,此方法不会失败。慎用
        return None
    def flushall(self, key):
        # flushall 删除所有数据库中的所有key，此方法不会失败。更加慎用
        return None
    
    # String 相关命令
    def set(self, key, value):
        # set key value 设置key对应的值为string类型的value,返回1表示成功，0失败
        return self.dbW.set(key, value)
    def setnx(self, key, value):
        # setnx key value 同上，如果key已经存在，返回0 。nx 是not exist的意思
        return self.dbW.setnx(key, value)
    def get(self, key):
        # get key 获取key对应的string值,如果key不存在返回nil
        return self.dbR.get(key)
    def getset(self, key, value):
        # getset key value  设置key的值，并返回key的旧值。如果key不存在返回nil
        return self.dbW.getset(key, value)
    def mget(self, *keys):
        # mget key1 key2 ... keyN 一次获取多个key的值，如果对应key不存在，则对应返回nil。下面是个实验, nonexisting不存在，对应返回nil
        return self.dbR.mget(*keys)
    def mset(self, key, mappings):
        # mset key1 value1 ... keyN valueN 一次设置多个key的值，成功返回1表示所有的值都设置了，失败返回0表示没有任何值被设置
        return self.dbW.mset(key, mappings)
    def msetnx(self, key, mappings):
        # msetnx key1 value1 ... keyN valueN 同上，但是不会覆盖已经存在的key
        return self.dbW.msetnx(key, mappings)
    def incr(self, key):
        # incr key 对key的值做加加操作,并返回新的值。注意incr一个不是int的value会返回错误，incr一个不存在的key，则设置key为1
        return self.dbW.incr(key)
    def decr(self, key):
        # decr key 同上，但是做的是减减操作，decr一个不存在key，则设置key为-1
        return self.dbW.decr(key)
    def incrby(self, key, integer):
        # incrby key integer 同incr，加指定值 ，key不存在时候会设置key，并认为原来的value是 0
        return self.dbW.incrby(key, integer)
    def decrby(self, key, integer):
        # decrby key integer 同decr，减指定值。decrby完全是为了可读性，我们完全可以通过incrby一个负值来实现同样效果，反之一样。
        return self.dbW.decrby(key, integer)
    def append(self, key, value):
        # append key value  给指定key的字符串值追加value,返回新字符串值的长度。
        return self.dbW.append(key, value)
    def substr(self, key, start, end):
        # substr key start end 返回截取过的key的字符串值,注意并不修改key的值。下标是从0开始的。
        return self.dbR.substr(key, start, end)

    # List
    # redis的list类型其实就是一个每个子元素都是string类型的双向链表。我们可以通过push,pop操作从链表的头部或者尾部添加删除元素。这使得list既可以用作栈，也可以用作队列。
    # list的pop操作还有阻塞版本的。当我们[lr]pop一个list对象是，如果list是空，或者不存在，会立即返回nil。但是阻塞版本的b[lr]pop可以则可以阻塞，当然可以加超时时间，超时后也会返回nil。为什么要阻塞版本的pop呢，主要是为了避免轮询。举个简单的例子如果我们用list来实现一个工作队列。执行任务的thread可以调用阻塞版本的pop去获取任务这样就可以避免轮询去检查是否有任务存在。当任务来时候工作线程可以立即返回，也可以避免轮询带来的延迟。 
    # List相关命令
    def lpush(self, key, string):
        # lpush key string 在key对应list的头部添加字符串元素，返回1表示成功，0表示key存在且不是list类型
        return self.dbW.lpush(key, string)
    def rpush(self, key, string):
        # rpush key string 同上，在尾部添加
        return self.dbW.rpush(key, string)
    def llen(self, key):
        # llen key 返回key对应list的长度，key不存在返回0,如果key对应类型不是list返回错误
        return self.dbR.llen(key)
    def lrange(self, key, start, end):
        # lrange key start end 返回指定区间内的元素，下标从0开始，负值表示从后面计算，-1表示倒数第一个元素 ，key不存在返回空列表
        return self.dbR.lrange(key, start, end)
    def ltrim(self, key, start, end):
        # ltrim key start end  截取list，保留指定区间内元素，成功返回1，key不存在返回错误
        return self.dbW.ltrim(key, start, end)
    def lset(self, key, index, value):
        # lset key index value 设置list中指定下标的元素值，成功返回1，key或者下标不存在返回错误
        return self.dbW.lset(key, index, value)
    def lrem(self, key, count, value):
        # lrem key count value 从key对应list中删除count个和value相同的元素。count为0时候删除全部
        return self.dbW.lrem(key, count, value)
    def lpop(self, key):
        # lpop key 从list的头部删除元素，并返回删除元素。如果key对应list不存在或者是空返回nil，如果key对应值不是list返回错误
        return self.dbW.lpop(key)
    def rpop(self, key):
        # rpop 同上，但是从尾部删除
        return self.dbW.rpop(key)
    def blpop(self, keysList, timeout):
        # blpop key1...keyN timeout 从左到右扫描返回对第一个非空list进行lpop操作并返回，比如blpop list1 list2 list3 0 ,如果list不存在，list2,list3都是非空则对list2做lpop并返回从list2中删除的元素。如果所有的list都是空或不存在，则会阻塞timeout秒，timeout为0表示一直阻塞。
        # 当阻塞时，如果有client对key1...keyN中的任意key进行push操作，则第一在这个key上被阻塞的client会立即返回。如果超时发生，则返回nil。
        return self.dbW.blpop(keysList, timeout) #??
    def brpop(self, keysList, timeout):
        # brpop 同blpop，一个是从头部删除一个是从尾部删除
        return self.dbW.brpop(keysList, timeout)
    def rpoplpush(self, srckey, destkey):
        # rpoplpush srckey destkey 从srckey对应list的尾部移除元素并添加到destkey对应list的头部,最后返回被移除的元素值，整个操作是原子的.如果srckey是空
        # 或者不存在返回nil
        return self.dbW.rpoplpush(srckey, destkey)

    # Set
    # redis的set是string类型的无序集合。
    # set元素最大可以包含(2的32次方-1)个元素。
    # set的是通过hash table实现的，hash table会随着添加或者删除自动的调整大小
    # 关于set集合类型除了基本的添加删除操作，其他有用的操作还包含集合的取并集(union)，交集(intersection)，差集(difference)。通过这些操作可以很容易的实现sns中的好友推荐和blog的tag功能。
    # Set相关命令
    def sadd(self, key, member):
        # sadd key member 添加一个string元素到,key对应的set集合中，成功返回1,如果元素以及在集合中返回0,key对应的set不存在返回错误
        return self.dbW.sadd(key, member)
    def srem(self, key, member):
        # srem key member 从key对应set中移除给定元素，成功返回1，如果member在集合中不存在或者key不存在返回0，如果key对应的不是set类型的值返回错误
        return self.dbW.srem(key, member)
    def spop(self, key):
        # spop key 删除并返回key对应set中随机的一个元素,如果set是空或者key不存在返回nil
        return self.dbW.spop(key)
    def srandmember(self, key):
        # srandmember key 同spop，随机取set中的一个元素，但是不删除元素
        return self.dbR.srandmember(key)
    def smove(self, srckey, dstkey, member):
        # smove srckey dstkey member 从srckey对应set中移除member并添加到dstkey对应set中，整个操作是原子的。成功返回1,如果member在srckey中不存在返回0，如果key不是set类型返回错误
        return self.dbW.smove(srckey, dstkey, member)
    def scard(self, key):
        # scard key 返回set的元素个数，如果set是空或者key不存在返回0
        return self.dbR.scard(key)
    def sismember(self, key, member):
        # sismember key member 判断member是否在set中，存在返回1，0表示不存在或者key不存在
        return self.dbR.sismember(key, member)
    def sinter(self, *keys):
        # sinter key1 key2...keyN 返回所有给定key的交集
        return self.dbR.sinter(*keys)
    def sinterstore(self, dstkey, *keys):
        # sinterstore dstkey key1...keyN 同sinter，但是会同时将交集存到dstkey下
        return self.dbW.sinterstore(dstkey, *keys)
    def sunion(self, *keys):
        # sunion key1 key2...keyN 返回所有给定key的并集
        return self.dbR.sunion(*keys)
    def sunionstore(self, dstkey, *keys):
        # sunionstore dstkey key1...keyN 同sunion，并同时保存并集到dstkey下
        return self.dbW.sunionstore(dstkey, *keys)
    def sdiff(self, *keys):
        # sdiff key1 key2...keyN 返回所有给定key的差集
        return self.dbR.sdiff(*keys)
    def sdiffstore(self, dstkey, *keys):
        # sdiffstore dstkey key1...keyN 同sdiff，并同时保存差集到dstkey下
        return self.dbW.sdiffstore(dstkey, *keys)
    def smembers(self, key):
        # smembers key 返回key对应set的所有元素，结果是无序的
        return self.dbR.smembers(key)

    # Sorted set
    # 和set一样sorted set也是string类型元素的集合，不同的是每个元素都会关联一个double类型的score。sorted set的实现是skip list和hash table的混合体。当元素被添加到集合中时，一个元素到score的映射被添加到hash table中，另一个score到元素的映射被添加到skip list
    # 并按照score排序，所以就可以有序的获取集合中的元素。
    # Sorted set 相关命令
    def zadd(self, key, score, member):
        # zadd key score member 添加元素到集合，元素在集合中存在则更新对应score
        return self.dbW.zadd(key, score, member)
    def zrem(self, key, member):
        # zrem key member 删除指定元素，1表示成功，如果元素不存在返回0
        return self.dbW.zrem(key, member)
    def zincrby(self, key, incr, member):
        # zincrby key incr member 增加对应member的score值，然后移动元素并保持skip list有序。返回更新后的score值
        return self.dbW.zincrby(key, incr, member)
    def zrank(self, key, member):
        # zrank key member 返回指定元素在集合中的排名（下标，非score）,集合中元素是按score从小到大排序的
        return self.dbR.zrank(key, member)
    def zrevrank(self, key, member):
        # zrevrank key member 同上,但是集合中元素是按score从大到小排序
        return self.dbR.zrevrank(key, member)
    def zrange(self, key, startScore, endScore):
        # zrange key start end 类似lrange操作从集合中取指定区间的元素。返回的是有序结果
        return self.dbR.zrange(key, startScore, endScore)
    def zrevrange(self, key, startScore, endScore):
        # zrevrange key start end 同上，返回结果是按score逆序的
        return self.dbR.zrevrange(key, startScore, endScore)
    def zrangebyscore(self, key, minScore, maxScore):
        # zrangebyscore key min max 返回集合中score在给定区间的元素
        return self.dbR.zrangebyscore(key, minScore, maxScore)
    def zcount(self, key, minScore, maxScore):
        # zcount key min max 返回集合中score在给定区间的数量
        return self.dbR.zcount(key, minScore, maxScore)
    def zcard(self, key):
        # zcard key 返回集合中元素个数
        return self.dbR.zcard(key)
    def zscore(self, key, element):
        # zscore key element  返回给定元素对应的score
        return self.dbR.zscore(key, element)
    def zremrangebyrank(self, key, minRank, maxRank):
        # zremrangebyrank key min max 删除集合中排名在给定区间的元素
        return self.dbW.zremrangebyrank(key, minRank, maxRank)
    def zremrangebyscore(self, key, minScore, maxScore):
        # zremrangebyscore key min max 删除集合中score在给定区间的元素
        return self.dbW.zremrangebyscore(key, minScore, maxScore)

    # Hash
    # redis hash是一个string类型的field和value的映射表。
    # hash特别适合用于存储对象。相较于将对象的每个字段存成单个string类型。将一个对象存储在hash类型中会占用更少的内存，并且可以更方便的存取整个对象。
    # Hash相关命令
    def hset(self, key, field, value):
        # hset key field value 设置hash field为指定值，如果key不存在，则先创建        
        return self.dbW.hset(key, field, value)
    def hget(self, key, field):
        # hget key field  获取指定的hash field
        return self.dbR.hget(key, field)
    def hmget(self, key, *fileds):
        # hmget key filed1....fieldN 获取全部指定的hash filed
        return self.dbR.hmget(key, *fileds)
    def hmset(self, key, mapping):
        # hmset key filed1 value1 ... filedN valueN 同时设置hash的多个field
        return self.dbW.hmset(key, mapping)
    def hincrby(self, key, field, integer):
        # hincrby key field integer 将指定的hash filed 加上给定值
        return self.dbW.hincrby(key, field, integer)
    def hexists(self, key, field):
        # hexists key field 测试指定field是否存在
        return self.dbR.hexists(key, field)
    def hdel(self, key, field):
        # hdel key field 删除指定的hash field
        return self.dbW.hdel(key, field)
    def hlen(self, key):
        # hlen key 返回指定hash的field数量
        return self.dbR.hlen(key)
    def hkeys(self, key):
        # hkeys key 返回hash的所有field
        return self.dbR.hkeys(key)
    def hvals(self, key):
        # hvals key 返回hash的所有value
        return self.dbR.hvals(key)
    def hgetall(self, key):
        # hgetall 返回hash的所有filed和value
        return self.dbR.hgetall(key)
    
    #发布订阅命令
    def psubscribe(self, *pattern):
        #PSUBSCRIBE pattern [pattern ...] 
        #订阅一个或多个符合给定模式的频道。
        return self.dbW.psubscribe(*pattern)
    def pubsub(self, *argument):
        #PUBSUB subcommand [argument [argument ...]] 
        #查看订阅与发布系统状态。
        return self.dbW.pubsub(*argument)
    def publish(self, channel, message):
        #PUBLISH channel message 
        #将信息发送到指定的频道。
        return self.dbW.publish(channel, message)
    def punsubscribe(self, *pattern):
        #PUNSUBSCRIBE [pattern [pattern ...]] 
        #退订所有给定模式的频道。
        return self.dbW.punsubscribe(*pattern)
    # def subscribe(self, *channel):
    #     #SUBSCRIBE channel [channel ...]
    #     #订阅给定的一个或多个频道的信息。
    #     return self.dbW.subscribe(*channel)
    # def unsubscribe(self, *channel):
    #     #UNSUBSCRIBE [channel [channel ...]]
    #     #指退订给定的频道。
    #     return self.dbW.unsubscribe(*channel)


class PipeHandle(RedisHandle):
    def __init__(self, redisHandle):
        #输入redis db对象
        self.dbW = redisHandle.dbW.pipeline()
        self.dbR = self.dbW

    def execute(self):
        return self.dbW.execute()

if __name__ == "__main__":
    dbW = getRedisDB(host="127.0.0.1",port=16379,db=15)
    dbR = getRedisDB(host="127.0.0.1",port=16379,db=15)
    redisDB = RedisHandle(dbW=dbW,dbR=dbR)
    pipeHandle = PipeHandle(redisDB)
    print (dbW)
    print (dbR)
    print (redisDB)
    print (pipeHandle)

