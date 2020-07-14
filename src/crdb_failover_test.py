import redis
import my_redis_constants
import time

REDIS_HOST1 = my_redis_constants.CRDB_HOST_INSTANCE1
REDIS_PORT1 = my_redis_constants.CRDB_PORT_INSTANCE1
REDIS_HOST2 = my_redis_constants.CRDB_HOST_INSTANCE2
REDIS_PORT2 = my_redis_constants.CRDB_PORT_INSTANCE2

class MyRedisClient():
    r = None
    current_host = REDIS_HOST1
    current_port = REDIS_PORT1

    # constructor
    def __init__(self):
        # establish client connection to host 1 
        self.connect_to_redis()

    def connect_to_redis(self):
        self.r = redis.StrictRedis(self.current_host, self.current_port, charset="utf-8", decode_responses=True, socket_timeout=10.0)
        print(f'Connected to Redis at host {self.current_host}:{self.current_port}')

    def switch_connection(self):
        # grab the connection info for the other cluster
        self.current_host = REDIS_HOST2 if (self.current_host == REDIS_HOST1) else REDIS_HOST1
        self.current_port = REDIS_PORT2 if (self.current_port == REDIS_PORT1) else REDIS_PORT1
        # reestablish connection to new cluster 
        self.connect_to_redis()

    def incr_counter(self):
        while True:
            try:
                # call Redis INCR command
                res = self.r.incr('mycounter')
                # print result and sleep 1 second
                print(f'Incremented counter on {self.current_host} to {res}')

            except redis.exceptions.ConnectionError:
                # in the case of a connection error, switch connection to the other cluster to continue writing
                print('Connection Error detected. Switching to other cluster.')
                self.switch_connection()

            except redis.exceptions.TimeoutError: 
                # in the case of a timeout error, wait 1 extra second and try the command again
                print('Timeout Error... trying again')
                time.sleep(1)

            time.sleep(1)

def main():
    # create client and start making calls to database 
    my_redis_client = MyRedisClient()
    my_redis_client.incr_counter()

    print('Exiting...')
        
if __name__ == "__main__":
    main()
