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
conn.setbit('key',2,1)
conn.bitcount('need to count', 0, 4)  # 统计0-4之间的为1的位数量
conn.bitop('and', 'destkey', 'key1', 'key2')


conn.lindex('listName', 10)
conn.ltrim('listName', 0, 5)  # 0-5之间的都会被包含
conn.lrange('listName', 0, -1)

# blocked:
conn.blpop('keyName', 10)  # 等待10毫秒
conn.brpop('keyName', 10)
conn.rpoplpush('key1', 'key2')
conn.brpoplpush('list1', 'list2', 30)


#  set
conn.sadd('set-key', 'a', 'b', 'c')
conn.srem('set-key', 'a', 'b')
conn.sismember('list', 'item')
conn.scard('set-key')
conn.smembers('listKey')
conn.srandmember('listKey', 20)
conn.spop('key')  # 随机返回
conn.smove('set-key1', 'set-key2', 'item')  # 把item 从 set-key1 移动到set-key2

conn.sdiffstore('storekey', 'set1', 'set2', 'set3')  # 把item从存在于第一个集合但不存在于其他集合中的元素存入storekey中
conn.sinter('sk1', 'sk2')  # 并集
conn.sinterstore('destination', 'set1', 'set2')
conn.sunion('k1', 'k2')  # 并集
conn.sunionstore('destination', 'k1', 'k2')  # 并集存储


# hash
conn.hmget('key-name', 'key1', 'key2', 'key3')
conn.hmset('key-name', 'key1', 'value1', 'key2', 'value2')
conn.hdel('key-name', 'key1', 'key2', 'key3')
conn.hlen('hash-key')  # 返回包含的值

conn.exists('hash-name', 'key')
conn.hkeys('hash-name')
conn.hvals('hash-name')
conn.hgetall('key-name')
conn.hincrby('keyname', 'key')
conn.hincrbyfloat('keyname', 'key', 'increment')

# zset
conn.zadd('zset-key', 'a', 3, 'b', 3, 'c', 4)
conn.zrem('zset-key', 'a')
conn.zcard('keyname')  # 返回有序集合包含的成员数量
conn.zincrby('key-name', 10, 'member')
conn.zcount('key-name', 10, 20)  # 找到10和20之间的成员数量
conn.zrank('key-name', 'mem_name')  # 返回mem_name的排名
conn.zscore('zset-key', 'member')  # 返回member的分值
conn.zrange('zset-key', 10, 20)


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


# expire
conn.persist('key-name')  # 移除键的过期时间
conn.ttl('time')  # 看给定键距离过期需要多久
conn.expire('keyName', 10)
conn.expireat('key-name', 10000)  # 设定UNIX时间戳
conn.pttl('key-name')  # 查看距离过期需要多少毫秒
conn.pexpire('key-name',320000)  # 在指定的毫秒后过期










