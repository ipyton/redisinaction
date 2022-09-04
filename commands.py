import time

import redis
import threading
conn = redis.Redis()

# string
conn.incr('a')
conn.incrby('a', 10)
conn.decr('a')
conn.decrby('a', 10)
conn.incrbyfloat('a', 20.0)

conn.append('name', 'append content')
conn.getrange('a long string', 3, 2)  # get substring
conn.setrange('a-new-storage', 3, 'd')
conn.getbit('keyname', 2)  # 看成二进制位，取该位内容
conn.setbit()
conn.bitcount()
conn.bitop()


conn.lindex()
conn.ltrim()
conn.lrange()




#  set
conn.sadd()
conn.srem()
conn.sismember()
conn.scard()
conn.smembers()
conn.srandmember()
conn.spop()
conn.smove()

conn.sdiffstore()
conn.sinter()
conn.sinterstore()
conn.sunion()
conn.sunionstore()


# hash
conn.hmget()
conn.hmset()
conn.hdel()
conn.hlen()

conn.exists()
conn.hkeys()
conn.hvals()
conn.hgetall()
conn.hincrby()
conn.hincrbyfloat()

# zset
conn.zadd()
conn.zrem()
conn.zcard()
conn.zincrby()
conn.zcount()
conn.zrank()
conn.zscore()
conn.zrange()

# pub/sub


def publisher(n):
    time.sleep(1)
    for i in range(n):
        conn.publish('channel', i)
        time.sleep(1)


def run_pubsub():
    threading.Thread(target=publisher, args=(3,)).start()
    pubsub = conn.pubsub()
    pubsub.subscribe(['channel'])
    count = 0
    for item in pubsub.listen():
        print(item)
        count += 1
        if count == 4:
            pubsub.unsubscribe()
        if count == 5:
            break

# other commands

conn.rpush('wait-for-sort', 23, 15, 11, 3423)
conn.sort('wait-for-sort')
conn.sort('wait-for-sort', alpha=True)

conn.hset('d-23', 'field', 23455)
conn.hset('d-15', 'field', 1532)
conn.hset('d-11', 'field', 11234)
conn.hset('d-3423', 'field', 3423)

conn.sort('wait-for-sort', by='d-*->field')  # 给
conn.sort('sort-input', by='d-*->field', get='d-*->field')  # 使用外部数据做返回值

# transaction


def trans():
    pipeline = conn.pipeline()
    pipeline.incr('trans')
    time.sleep(.1)

    pipeline.incr('trans:', -1)
    print(pipeline.execute([0]))


if 1:
    for i in range(3):
        threading.Thread(target=trans).start()
    time.sleep(.5)







