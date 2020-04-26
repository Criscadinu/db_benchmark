import happybase
from datetime import datetime
from Databases import Database



class Happybase(Database):
    def __init__(self):
        self.backlog_count_ = 0
        self.backlog_upper_bound_ = 20

def connection(self):
    conn = happybase.Connection(host = host)
    conn.open()
    return conn

def write(self, drone_update):
    print(self.conn.tables())
    self.table = connection.table('PARIS')
    self.batch = table.batch()
    self.batch.put(datetime.datetime, {'drone:id':  drone_update.drone_id, 'drone_lat': drone_update.drone_lat, 'drone_long': drone_update.drone_long,'batterij_duur': drone_update.batterij_duur})
    self.batch.delete(b'row-key-4')
    self.batch.send()
    return
