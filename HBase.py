import happybase
from datetime import datetime
from Databases import Database
from DroneData import *



class Happybase(Database):
    def __init__(self, setting):
        self.
        self.backlog_count_ = 0
        self.backlog_upper_bound_ = 20
        self.table_ = None
        self.batch_ = None

        self.batch_size_ = int(setting.get('batch_size'))

        self.connection = None

def connection(self):
    conn = happybase.Connection(host = host,
                                table_prefix = namespace,
                                table_prefix_separator = ":")
    conn.open()
    table = conn.table(table_name)
    batch = table.batch(batch_size = batch_size)
    return conn, batch


    uitvoering = {
        "tijd": datetime.datetime.utcnow(),
        "drone_id": "drone" + str(drone_update.drone_id),
        "drone_lat": drone_update.drone_lat,
        "drone_long": drone_update.drone_long,
        "batterij_duur": drone_update.batterij_duur
    }
def write(self, drone_update):
    print('Writing ...')
    put 'PARIS', datetime.datetime, 'drone:id',  + str(drone_update.drone_id), 'drone_lat': drone_update.drone_lat,'drone_long': drone_update.drone_long, 'batterij_duur': drone_update.batterij_duur
    return
