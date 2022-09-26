# log process
import bisect
import contextlib
import csv
import functools
import json
import logging
import os
import time
import datetime
import uuid
import math
import redis

'''
用于形参时
* 未匹配的位置参数
** 未匹配的关键字参数
'''


conn = redis.Redis("8.8.8.8")

SEVERITY = {
    logging.DEBUG: 'debug',
    logging.INFO: 'info',
    logging.WARNING: 'warning',
    logging.ERROR: 'error',
    logging.CRITICAL: 'critical'
}

SEVERITY.update((name, name) for name in SEVERITY.values())


def log_recent(name, message, severity=logging.INFO, pipe=None):
    severity = str(SEVERITY.get(severity, severity)).lower()
    destination = 'recent:%s:%s' % (name, severity)
    message = time.asctime() + ' ' + message
    pipe = pipe or conn.pipeline()
    pipe.lpush(destination, message)
    pipe.ltrim(destination, 0, 99)
    pipe.execute()


def log_common( name, message, severity = logging.INFO, timeout = 5):
    severity = str(SEVERITY.get(severity)).lower()
    destination = 'common:%s:%s' % (name, severity)
    start_key = destination + ':start'

    pipe = conn.pipeline()
    end = time.time()+timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()

            existing = pipe.get(start_key)
            pipe.multi()
            # 同类型的键如果在之前出现过，则把目前的key变为从前的，并且设置当前的key为最新的
            if existing and existing < hour_start:
                pipe.rename(destination, destination+':last')
                pipe.rename(start_key, destination+':pstart')
                pipe.set(start_key, hour_start)
            elif not existing:
                pipe.set(start_key, hour_start)
            pipe.zincrby(destination, hour_start)
            return
        except redis.exceptions.WatchError:
            continue


PRECISION = [1, 5, 60, 300, 3600, 18000, 86400]


def update_counter(name, count=1, now=None):
    now = now or time.time()
    pipe = conn.pipeline()
    for prec in PRECISION:
        pnow = int(now/prec) * prec
        hash = '%s%s' % (prec, name)
        pipe.zadd('known:', hash, 0)
        pipe.hincrby('count:' + hash, pnow, count)
    pipe.execute()


def get_counter(name, precision):
    hash = '%s:%s' % (precision, name)
    data = conn.hgetall('count:' + hash)
    to_return = []
    for key, value in data.iteritems():
        to_return.append((int(key), int(value)))
    to_return.sort()
    return to_return


SAMPLE_COUNT = 10
def clean_counters():
    pipe = conn.pipeline(True)
    passes = 0
    QUIT = False
    while not QUIT:
        start = time.time()
        index = 0 # 设置index多此一举
        while index < conn.zcard('known'):
            hash = conn.zrange('known:', index, index) # 把按分数排名index-index的内容取出来
            index += 1
            if not hash:
                break
            hash = hash[0]
            prec = int(hash.partition(':')[0])
            bprec = int(prec // 60 ) or 1
            if passes % bprec: #
                continue

            hkey = 'count:' + hash
            cutoff = time.time() -SAMPLE_COUNT * prec #计算保留什么时间之后的样本
            samples = map(int, conn.hkeys(hkey)) # 把每个拿到的key都当作数字
            samples.sort()
            remove = bisect.bisect_right(samples, cutoff) #return where to insert cutoff in samples assuming it is sorted.

            if remove:
                conn.hdel(hkey, *samples[:remove])
                if remove == len(samples):
                    try:
                        pipe.watch(hkey)
                        if not pipe.hlen(hkey):
                            pipe.multi()
                            pipe.zrem('known:', hash)
                            pipe.execute()
                            index -= 1
                        else:
                            pipe.unwatch()
                    except redis.exceptions.WatchError:
                        pass

        passes+=1
        duration = min(int(time.time() -start) + 1, 60)
        time.sleep(max(60-duration), 1)


# 存储统计数据
def update_stats(context, type, value, timeout = 5):
    destination = 'stats:%s:%s' % (context, type)
    start_key = destination + ':start'
    pipe = conn.pipeline(True)
    end = time.time() + timeout
    while time.time() < end:
        try:
            pipe.watch(start_key)
            now = datetime.utcnow().timetuple()
            hour_start = datetime(*now[:4]).isoformat()

            existing = pipe.get(start_key)
            pipe.multi()

            if existing and existing < hour_start:
                pipe.rename(destination, destination + ':last')
                pipe.rename(start_key, destination + ':pstart')
                pipe.set(start_key, hour_start)

            tkey1 = str(uuid.uuid4())
            tkey2 = str(uuid.uuid4())
            pipe.zadd(tkey1, 'min', value)
            pipe.zadd(tkey2, 'max', value)

            pipe.zunionstore(destination, [destination, tkey1], aggregate='min')
            pipe.zunionstore(destination, [destination, tkey2], aggregate='max')

            pipe.delete(tkey1, tkey2)
            pipe.zincrby(destination, 'count')
            pipe.zincrby(destination, 'sum', value)
            pipe.zincrby(destination, 'sumsq', value*value)

            return pipe.execute()[-3:]
        except redis.exceptions.WatchError:
            continue


def get_stats(context, type):
    key = 'stats:%s:%s' % (context, type)
    data = dict(conn.zrange(key, 0, -1, withscores=True))
    data['average'] = data['sum'] / data['count']
    numerator = data['sumsq'] - data['sum']**2 / data['count']
    data['stddev'] = (numerator / (data['count'] - 1 or 1)) ** .5
    return data


@contextlib.contextmanager
def access_time(context):
    start = time.time()
    yield

    delta = time.time() - start
    stats = update_stats(conn, context, 'AccessTime', delta)
    average = stats[1]/stats[0]

    pipe = conn.pipeline(True) # 返回一个可以支持事务的客户端
    pipe.zadd('slowest:AccessTime', context, average)
    pipe.zremrangebyrank('slowest:AccessTime', 0, -101)
    pipe.execute()


# shows usage of context man ager
def process_view(callback):
    with access_time(conn):
        return callback



# find ip and its country
def ip_to_score(ip_address):
    score = 0
    for v in ip_address.split('.'):
        score = score * 256 + int(v, 10)
    return score


def import_ips_to_redis(file_name):
    csv_file = csv.reader(open(file_name, 'rb'))
    for count, row in enumerate(csv_file):
        start_ip = row[0] if row else ' '
        if 'i' in start_ip.lower():
            continue
        if '.' in start_ip:
            start_ip = int(start_ip, 10)
        elif start_ip.isdigit():
            start_ip = int(start_ip, 10)
        else : continue
        city_id = row[2] + '_' +str(count)
        conn.zadd('ip2cityid:', city_id, start_ip)


def import_cities_to_redis(filename):
    for row in csv.reader(open(filename, 'rb')):
        if len(row) < 4 or not row[0].isdigit():
            continue
        row = [i.decode('latin-1') for i in row]
        city_id = row[0]
        country = row[1]
        region = row[2]
        city = row[3]
        conn.hset('cityid2city:', city_id, json.dumps([city, region, country])) #json.dumps 转化为json格式字符串


def find_city_by_ip(ip_address):
    if isinstance(ip_address, str):
        ip_address = ip_to_score(ip_address)

    city_id = conn.zrevrangebyscore('ip2cityid:', ip_address, 0, start=0, num=1)
    if not city_id:
        return None

    city_id = city_id[0].partition('_')[0]

    return json.loads(conn.hget('cityid2city:', city_id))


# 配置中心

LAST_CHECKED = None
IS_UNDER_MAINTENANCE = False


def is_under_maintenance():
    global LAST_CHECKED, IS_UNDER_MAINTENANCE

    if LAST_CHECKED < time.time() - 1:
        LAST_CHECKED = time.time()
        IS_UNDER_MAINTENANCE = bool(
            conn.get('is-under-maintenance')
        )
        return IS_UNDER_MAINTENANCE


def set_config(type, component, config):
    conn.set('config:%s:%s' % (type, component),json.dumps(config))


CONFIGS = {}
CHECKED = {}


def get_config(type, component, wait=1):
    key = 'config:%s:%s' % (type, component)
    if CHECKED.get(key) < time.time() - wait: # 已经超时
        CHECKED[key] = time.time()
        config = json.loads(conn.get(key) or '{}')
        config = dict((str(k), config[k]) for k in config)
        old_config = CONFIGS.get(key)

        if config != old_config:
            CONFIGS[key] = config
    return CONFIGS.get(key)


REDIS_CONNECTIONS = {}


# 有参装饰器怎么办？ 套三层,做的是，封装了提前查redis的逻辑。
def redis_connection(component, wait=1):
    key = 'config:redis:' +component
    def wrapper(function):
        @functools.wraps(function)
        def call(*args, **kwargs):
            old_config = CONFIGS.get(key, object())
            _config = get_config('redis', component, wait)
            config = {}
            for k, v in _config.iteritems():
                config[k.encode('utf-8')] = v

            if old_config != _config:
                REDIS_CONNECTIONS[key] = redis.Redis(**config)
            return function(REDIS_CONNECTIONS.get(key), *args, **kwargs)
        return call
    return wrapper


#
@redis_connection('logs')
def log_recent(conn, app, message):
    'the old log_recent() code'


#自动补全联系人
def add_update_contact(user, contact):
    ac_list = 'recent:' + user
    pipeline = conn.pipeline(True)
    pipeline.lrem(ac_list, contact)
    pipeline.lpush(ac_list, contact)  # 更新
    pipeline.ltrim(ac_list, 0, 99)
    pipeline.execute()


def remove_contact(user, contact):
    conn.lrem('recent:' + user, contact)


def fetch_autocompelete_list(user, prefix):
    candidates = conn.lrange('recent:' + user, 0, -1)
    matches = []
    for candidate in candidates:
        if candidate.lower().startswith(prefix):
            matches.append(candidate)
    return matches



'''
ab` 位于aba 之前 ，aba{位于abaxxxx之后。
这个做法太聪明了!!!!
'''
valid_charaters = '`abcdefghijklmnopqrstuvwxyz{'
def find_prefix_range(prefix):
    posn = bisect.bisect_left(valid_charaters, prefix[-1:])
    suffix = valid_charaters[(posn or 1) - 1]
    return prefix[:-1] + suffix + '{',prefix + '{'



def autocomplete_on_prefix(guild, prefix):
    start, end = find_prefix_range(prefix)
    identifier = str(uuid.uuid4())
    start += identifier
    end += identifier

    zset_name = 'members:' + guild

    conn.zadd(zset_name, start, 0 ,end, 0)
    pipeline = conn.pipeline(True)
    while True:
        try:
            pipeline.watch(zset_name)
            sindex = pipeline.zrank(zset_name, start)
            eindex = pipeline.zrank(zset_name, end)
            erange = min(sindex + 9, eindex - 2)
            pipeline.multi()
            pipeline.zrem(zset_name, start, end)
            pipeline.zrange(zset_name, sindex, erange)
            items = pipeline.execute()[-1]
            break
        except redis.exceptions.WatchError:
            continue
    return [item for item in items if '{' not in item]


def join_guild(guild, user):
    conn.zadd('members:' + guild, user, 0)


def leave_guild(guild, user):
    conn.zrem('members:' + guild, user)


# distributed lock
def acquire_lock(lockname, acquire_timeout = 10):
    identifier = str(uuid.uuid4())
    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx():
            return identifier
        time.sleep(.001)
    return False


def purchase_item_with_lock(buyerid, itemid, sellerid):
    buyer = "users:%s" % buyerid
    seller = "users:%s" % sellerid
    item = "%s.%s" % (itemid, sellerid)
    inventory = "inventory:%s" % buyerid

    locked = acquire_lock(sellerid)
    if not locked:
        return False
    pipe = conn.pipeline(True)
    try:
        pipe.zscore("market:", item)
        pipe.hget(buyer, 'funds')
        price, funds = pipe.execute()
        if price is None or price > funds:
            return None

        pipe.hincrby(seller, 'funds', int(price))
        pipe.hincrby(buyer, 'funds', int(-price))
        pipe.sadd(inventory, itemid)
        pipe.zrem('market:', item)
        pipe.execute()
        return True
    finally:
        release_lock(sellerid, locked)


def release_lock(lockname, identifier):
    pipe = conn.pipeline(True)
    lockname = 'lock:' + lockname

    while True:
        try:
            pipe.watch(lockname) # 可以设置锁的粒度
            if pipe.get(lockname) == identifier:
                pipe.multi()
                pipe.delete(lockname)
                pipe.execute()
                return True
            pipe.unwatch()
            break
        except redis.exceptions.WatchError:
            pass

    return False


def acquire_lock_with_timeout(lockname, acquire_timeout=10, lock_timeout=10):
    identifier = str(uuid.uuid4())
    lockname = 'lock:' + lockname
    lock_timeout = int(math.ceil(lock_timeout))

    end = time.time() + acquire_timeout
    while time.time() < end:
        if conn.setnx(lockname, identifier):
            conn.expire(lockname, lock_timeout)
            return identifier
        elif not conn.ttl(lockname):
            conn.expire(lockname, lock_timeout)
        time.sleep(.001)
    return False


def acquire_semaphore(semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    now = time.time()

    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout) # 清理过期信号量持有者
    pipeline.zadd(semname, identifier, now)
    pipeline.zrank(semname, identifier) # 检查是否成功获得了信号量
    if pipeline.execute()[-1] < limit:
        return identifier

    conn.zrem(semname, identifier)  # 获取信号量失败
    return None


def release_semaphore(semname, identifier):
    return conn.zrem(semname, identifier)


def acquire_fair_semaphore(semname, limit, timeout=10):
    identifier = str(uuid.uuid4())
    czset = semname + ':owener'
    ctr = semname + ':counter'

    now = time.time()
    pipeline = conn.pipeline(True)
    pipeline.zremrangebyscore(semname, '-inf', now - timeout)
    pipeline.zinterstore(czset, {czset: 1, semname: 0})

    pipeline.incr(ctr)
    counter = pipeline.execute()[-1]

    pipeline.zadd(semname, identifier, now)
    pipeline.zadd(czset, identifier, counter)

    pipeline.zrank(czset, identifier)

    if pipeline.execute()[-1] < limit:
        return identifier
    pipeline.zrem(semname, identifier, now)
    pipeline.zrem(czset, identifier)
    pipeline.execute()
    return None


def release_fair_semaphore(semname, identifier):
    pipeline = conn.pipeline(True)
    pipeline.zrem(semname, identifier)
    pipeline.zrem(semname + ':owner'. identifier)
    return pipeline.execute()[0]


def refresh_fair_semaphore(semname, identifier):
    if conn.zadd(semname, identifier, time.time()):
        release_fair_semaphore(semname, identifier)
        return False
    return True


def acquire_semaphore_with_lock(semname, limit, timeout=10):
    identifier = acquire_lock(semname, acquire_timeout= .01)
    if identifier:
        try:
            return acquire_fair_semaphore(semname, limit, timeout)
        finally:
            release_lock(semname, identifier)


# task queue
def send_sold_email_via_queue(seller, item, price, buyer):
    data = {
        'seller_id': seller,
        'item_id': item,
        'price': price,
        'buyer_id': buyer,
        'time': time.time()
    }
    conn.rpush('queue:email', json.dumps(data))


def fetch_data_and_send_sold_email(content):
    # do something to send email
    pass


QUIT = False
def process_sold_email_queue():
    while not QUIT:
        packed = conn.blpop(['queue:email'], 30)

        if not packed:
            continue

        to_send = json.loads(packed[1])
        try:
            fetch_data_and_send_sold_email(to_send)
        except Exception:
            print("failed!!")
        else:
            print('success')


def worker_watch_queue(queue, callbacks):
    while not QUIT:
        packed = conn.blpop([queue], 30)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            print("Unknown callback %s" % name)
            continue
        callbacks[name](*args) # 解包裹


def worker_watch_queues(queue, callbacks):
    while not QUIT:
        packed = conn.blpop(queue, 30)
        if not packed:
            continue
        name, args = json.loads(packed[1])
        if name not in callbacks:
            print("error")
            continue
        callbacks[name](*args)

def execute_later(queue, name, args, delay=0):
    identifier =str(uuid.uuid4())
    item = json.dumps([identifier, queue, name, args])
    if delay> 0:
        conn.zadd('delayed:', item, time.time() + delay)
    else:
        conn.rpush('queue:' + queue, item)
    return identifier

#从延时队列中取数据出来
def poll_queue():
    while not QUIT:
        item = conn.zrange('delayed:', 0, 0,withscores=True)
        if not item or item[0][1] > time.time():
            time.sleep(.01)
            continue

        item = item[0][0]
        identifier, queue, function, args = json.loads(item)

        locked = acquire_lock(identifier)
        if not locked:
            continue
        if conn.zrem('delay:', item):
            conn.rpush('queue:' + queue, item)
        release_lock(identifier, locked)

# 优先级队列 + 延时队列

# message pull
def create_chat(sender, recipients, message, chat_id=None):

def send_message(chat_id, sender, message):

def fetch_pending_messages(recipient):

def join_chat(chat_id, user):

def leave_chat(char_id, user):


aggregates = defaultdict(lambda: defaultdict(int))
# distribute the files
def daily_country_aggregate(line):
    if line:
        line = line.split()
        ip = line[0]
        day = line[1]
        country = find_city_by_ip(ip)[2]
        aggregates[day][country] += 1
        return
    for day, aggregate in aggregates.items():
        conn.zadd('daily:country:' + day, **aggregate)
        del aggregates[day]

def copy_logs_to_redis(path, channel, count=10, limit=2**30):
    bytes_in_redis = 0
    waiting = deque()
    create_chat()
    count = str(count)
    for logfile in sorted(os.listdir(path)):
        full_path = os.path.join(os.listdir)



def process_log_from_redis(id, callback):
    while True:


def readlines(key, rblocks):
    out = ''
    for block in rblocks(key):
        out += block
        posn = out.rfind('\n')
        if posn >= 0:
            for line in out[:posn].split('\n'):
                yield line + '\n'
            out = out[posn+1:]
        if not block:
            yield out
            break


def readblocks(key, blocksize=2**17):
    lb = blocksize
    pos = 0
    while lb == blocksize:
        block = conn.substr(key, pos, pos + blocksize - 1)
        yield block
        lb = len(block)
        pos += lb
    yield ''

# 压缩一下
def readblocks_gz(key):
    inp = ''
    decoder = None
    for block in readblocks(key, 2**17):
        if not decoder:
            inp += block
            try:
                if inp[:3] != '\x1f\x8b\x08': # ????
                    raise IOError("invalid gzip data")
                i = 10
                flag = ord(inp[3])

                if flag & 4:
                    i += 2 + ord(inp[i]) + 256*ord(inp[i+1])
                if flag & 8:
                    i = inp.index('\0', i) + 1
                if flag & 16:
                    i = inp.index('\0', i) + 1
                if flag & 2:
                    i += 2

                if i > len(inp):
                    raise IndexError("not enough data")
            except (IndexError, ValueError):
                continue

            else:
                block = inp[i:]

