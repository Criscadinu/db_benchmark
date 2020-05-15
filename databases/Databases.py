import abc
import datetime
import mysql.connector as mariadb
from pymongo import MongoClient
from bson import DBRef
from DroneData import *
import redis
import pymonetdb

class Database(object):
    """
    Abstracte parent klasse voor database implementaties.
    Dient meer als interface dan als klasse, maar interfaces bestaan niet in Python.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def connect(self, ipv4):
        """
        Abstracte methode om verbinding te maken met de database
        :param ipv4: IP adres van de host van de database
        :param database_name: de naam van de database (zoals top2000db, gameparadise, etc)
        :return: Niks
        """
        return

    @abc.abstractmethod
    def write(self, drone_update, id):
        """
        Abstracte methode om een nieuwe drone update naar te database te schrijven.
        :param drone_update: datastructuur waarin de data van de drone update zit
        :return: Niks
        """
        return

    @abc.abstractmethod
    def read(self, aantal_records):
        """
        Abstracte methode om een gegeven aantal drone updates uit de database te lezen.
        :param aantal_records: het aantal records dat moet worden uitgelezen
        :return: Het aantal records
        """
        return

    @abc.abstractmethod
    def count_records(self):
        """
        Abstracte methode om het aantal relevante records te tellen
        :return:
        """
        return

    @abc.abstractmethod
    def empty(self):
        """
        Abstracte methode om de database leeg te maken voor een volgende test
        :return: Niks
        """
        return


class SQL(Database):
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self, ipv4):
        self.connection = mariadb.connect(host=ipv4, port=3306, user='test', password='test123', database="paris")
        self.cursor = self.connection.cursor()

    def write(self, drone_update):
        self.cursor.execute("insert into UITVOERING values (%s, %s, FROM_UNIXTIME(%s), %s, %s, NULL, NULL, %s)",
                            (drone_update.uitvoering_id,
                             drone_update.drone_id,
                             drone_update.timestamp,
                             drone_update.drone_lat,
                             drone_update.drone_long,
                             drone_update.batterij_duur
                             ))
        self.connection.commit()
        return

    def read(self, aantal_records):
        self.cursor.execute("select * from UITVOERING LIMIT " + str(aantal_records))
        return self.cursor.fetchall()

    def empty(self):
        self.cursor.execute("delete from UITVOERING")
        self.connection.commit()
        return

    def count_records(self):
        self.cursor.execute("select count(*) from UITVOERING")
        return self.cursor.fetchone()[0]


class MongoDB(Database):

    def __init__(self):
        self.client = None
        self.db = None
        self.uitvoering_col = None

    def connect(self, ipv4):
        self.client = MongoClient(ipv4, 27017)
        self.db = self.client['paris']
        self.uitvoering_col = self.db["uitvoering"]

    def write(self, drone_update):
        new_entry = {
            "tijd": datetime.datetime.utcnow(),
            "drone_id": DBRef("drone", str(drone_update.drone_id)),
            "drone_lat": drone_update.drone_lat,
            "drone_long": drone_update.drone_long,
            "batterij_duur": drone_update.batterij_duur
        }
        self.uitvoering_col.insert_one(new_entry)
        return

    def read(self, aantal_records):
        for i in self.uitvoering_col.find().limit(aantal_records):
            pass
        return 

    def empty(self):
        self.uitvoering_col.drop()
        return

    def count_records(self):
        return self.uitvoering_col.count()


class Redis(Database):
    def __init__(self):
        self.connection = None
        self.pipe = None
        self.keys = None

    def connect(self, ipv4):
        self.connection = redis.StrictRedis(host=ipv4, port=6379, db=0, decode_responses=True)
        self.pipe = self.connection.pipeline()
#        self.keys = self.connection.keys('*')

    def write(self, drone_update):
        uitvoering = {
            "tijd": str(datetime.datetime.utcnow()),
            "drone_id": "drone" + str(drone_update.drone_id),
            "drone_lat": drone_update.drone_lat,
            "drone_long": drone_update.drone_long,
            "batterij_duur": drone_update.batterij_duur
        }
        self.connection.hmset(
            "uitvoering" + uitvoering.get("tijd"), uitvoering)
        return

    def empty(self):
        self.connection.flushall()

    def read(self, aantal_records):
        i = 0
        for key in self.connection.scan_iter():
            self.pipe.hgetall(key)
            i = i + 1
            if aantal_records == i: break
        self.pipe.execute()

    def count_records(self):
        return self.connection.dbsize()


class Monetdb(Database):
    def __init__(self):
        self.connection = None
        self.cursor = None

    def connect(self,  ipv4):
        self.connection = pymonetdb.connect(
            username="monetdb", password="monetdb", hostname="localhost", database="paris")
        self.cursor = self.connection.cursor()

    def write(self, drone_update):
        self.cursor.execute("insert into UITVOERING values (%s, %s, sys.epoch(%s), %s, %s, NULL, NULL, %s)" %
                            (drone_update.uitvoering_id,
                             drone_update.drone_id,
                             drone_update.timestamp,
                             drone_update.drone_lat,
                             drone_update.drone_long,
                             drone_update.batterij_duur
                             ))

        self.connection.commit()
        return

    def empty(self):
        self.cursor.execute("delete from UITVOERING")
        self.connection.commit()
        return

    def read(self, aantal_records):
        self.cursor.execute("select * from UITVOERING LIMIT " + str(aantal_records))
        return self.cursor.fetchall()
