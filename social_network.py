import redis
import time
from supportApplications import *
conn = redis.Redis('localhost:8080')

def create_user(login, name):
    llogin = login.lower()
    lock = acquire_lock_with_timeout(conn, 'user:' + llogin, 1)
    if not lock:
        return None

    if conn.get('users:', llogin):
        release_lock('user:' + llogin,lock)
        return None
    id = conn.incr("user:id:")
    pipeline = conn.pipeline(True)
    pipeline.hset('user:', llogin, id)
    pipeline.hmset('user:%s' % id, {
        'login': login,
        'id': id,
        'name': name,
        'followings': 0,
        'followers': 0,
        'posts': 0,
        'signup': time.time()
    })

    pipeline.execute()
    release_lock('user:' + llogin, lock)
    return id

def create_status(uid, message, **data):
    pipeline = conn.pipeline(True)
    pipeline.hget('user:%s' % uid, 'login')
    pipeline.incr('status:id:id')
    login, id = pipeline.execute()

    if not login:
        return None

    data.update({
        'message': message,
        'posted': time.time(),
        'id': id,
        'uid': uid,
        'login': login
    })

    pipeline.hmget('status:%s'%id, data)
    pipeline.hincrby('user:%s'%uid, 'posts')
    pipeline.execute()
    return id

def get_status_message():

def follow_user():

def unfollow_user():

def post_status():

def syndicate_status():

def delete_status():

# stream API
class StreamAPIServer:


def parse_identifier():

def process_filter():


def create_status():


def delete_status():

def filter_content():

def create_filter():


def SampleFilter():

def TrackFilter():

def Follower():

def LocationFilter():

def