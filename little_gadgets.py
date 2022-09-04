import time

import redis

# handle login issues
conn = redis.Redis()


def check_token(token):
    return conn.hget('login:', token)


def update_token(token, user, item=None):
    timestamp = time.time()
    conn.hset('login:', token, user)
    conn.zadd('recent:', token, timestamp)

    # 记录浏览过的商品
    if item:
        conn.zadd('viewed:'+token, user)
        conn.zremrangebyrank('viewed:', token, 0, -26)


QUIT = False
LIMIT = 10000000

# another thread clean the redis DB


def clean_sessions():
    while not QUIT:
        size = conn.zcard()
        if size <= LIMIT:
            time.sleep(1)
            continue
        end_index = min(size-LIMIT, 100) #

        tokens = conn.zrange('recent:', 0, end_index-1)
        session_keys = []
        for token in tokens:
            session_keys.append('viewd:'+token)
        # 作用：把数组中的每一个数都拿出来调用一边这个方法
        conn.delete(*session_keys)

        conn.hdel('login:', *tokens)
        conn.zrem('recent:', *tokens)

