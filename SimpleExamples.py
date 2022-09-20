import json

import redis
import time
conn = redis.Redis()

QUIT = True
LIMIT = 20

def add_to_cart(session, item, count):
    if count <= 0:
        conn.hrem('cart:', session, item)
    else:
        conn.hset('cart:'+session,item,count)

# 删除商品和会话 , commodities would be dropped if session is invalid,it makes no sense

def clean_full_sessions():
    while not QUIT:
        size = conn.zcard('recent:')
        if size <= LIMIT:
          time.sleep(1)
          continue
        end_index = min(size-LIMIT, 100)
        sessions = conn.zrange('recent:', 0, end_index-1)

        session_keys = []
        for sess in sessions:
            session_keys.append('viewed:'+sess)
            session_keys.append('cart:'+sess)

        conn.delete(*session_keys)
        conn.hdel('login:',*sessions)
        conn.zrem('recent:',*sessions)


# web cache

def cache_request(request, callback):
    if not can_cache(conn,request):
        return callback(request)
    page_key = 'cache:'+hash_request(request)
    content = conn.get(page_key)
    if not content:
        content = callback(request)
        conn.setex(page_key, content, 300)
    return content


# data row cache

def schedule_row_cache(row_id, delay):
    conn.zadd('delay:', row_id, delay)
    conn.zadd('schedule:',row_id,time.time())

def cache_rows():
    while not QUIT:
        next = conn.zrange('schedule:', 0, 0, withscores=True)
        now = time.time()
        if not next or next[0][1] > now:
            time.sleep(.5)
            continue
        row_id = next[0][0]

        delay = conn.zscore('delay:', row_id)
        if delay <= 0:
            conn.zrem("delay:",row_id)
            conn.zrem("schedule",row_id)
            conn.delete("inv:"+row_id)
            continue
        row = Inventory.get(row_id)
        conn.zadd('schedule:',row_id,now+delay)
        conn.set('inv:'+row_id,json.dumps(row.to_dict))

# website analysis

def limited_update_token(token, usr, item=None):
    timestamp = time.time()
    conn.hset('login:', token, usr)
    conn.zadd('recent:', token, timestamp)

    # 记录浏览过的商品
    if item:
        conn.zadd('viewed:' + token, usr)
        conn.zremrangebyrank('viewed:', token, 0, -26)
        conn.zincrby('view:', item, -1)

def rescale_viewed():
    while not QUIT:
        conn.zremrangebyrank('viewed:',0,-200001)
        conn.zinterstore('viewed:',{'viewed:': .5})
        time.sleep(300)

def extract_item_id(request):
    return None

def is_dynamic(request):
    return None

def can_cache(request):
    item_id = extract_item_id(request)
    if not item_id or is_dynamic(request):
        return False
    rank = conn.zrank('viewed:', item_id)
    return rank is not None and rank < 10000





def hash_request():
