import redis
from datetime import datetime
from Databases import Database
from DroneData import *


class Redis(Database):
    def __init__(self):
        self.connection = None
        self.now = datetime.now()


def connect(self, ipv4, database_name):
    print('hello')
    self.connection = redis.Redis(host=ipv4, port=6379, db=0)


def write(self, drone_update):
    print('hello')
    uitvoering = {
        "tijd": datetime.datetime.utcnow(),
        "drone_id": "drone" + str(drone_update.drone_id),
        "drone_lat": drone_update.drone_lat,
        "drone_long": drone_update.drone_long,
        "batterij_duur": drone_update.batterij_duur
    }
    self.connection.hmset("uitvoering", uitvoering)
    return
