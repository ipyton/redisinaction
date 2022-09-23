import redis
import
conn = redis.Redis('localhost:8080')

def create_user(login, name):
    llogin = login.lower()
    lock = acquire_lock_with_timeout(conn, 'user:' + llogin, 1)
    if not lock:
        return None

    if conn.get('users:', llogin):
        release_lock()


def create_status():

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