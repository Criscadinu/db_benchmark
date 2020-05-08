import happybase
from datetime import datetime
from Databases import Database



class Monetdb(Database):
    def __init__(self):
        self.connection = None

    def connect(self,  ipv4,  database_name):
        self.connection = pymonetdb.connect(username="monetdb", password="monetdb", hostname="localhost", database="paris")


    def write(self, drone_update):
        self.connection.execute("INSERT INTO UITVOERING VALUES(0,1,CURRENT_TIMESTAMP,0.23,3.534,'pathHDB', 'pathWB', 88)")

        self.connection.commit()
        return
    def read(self, n_records):
        self.connection.execute("select * from UITVOERING LIMIT" + n_records)
        print(self.connection.execute("select * from UITVOERING LIMIT" + n_records) )
        return
