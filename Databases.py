import abc
import datetime
import mysql.connector as mariadb
from pymongo import MongoClient
from bson import DBRef
from DroneData import *


class Database(object):
    """
    Abstracte parent klasse voor database implementaties.
    Dient meer als interface dan als klasse, maar interfaces bestaan niet in Python.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def connect(self, ipv4, database_name):
        """
        Abstracte methode om verbinding te maken met de database
        :param ipv4: IP adres van de host van de database
        :param database_name: de naam van de database (zoals top2000db, gameparadise, etc)
        :return: Niks
        """
        return

    @abc.abstractmethod
    def write(self, drone_update):
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

    def connect(self, ipv4, database_name):
        self.connection = mariadb.connect(host=ipv4, port=3306, user='root', password='test123',
                                          database=database_name)
        self.cursor = self.connection.cursor()

    def write(self, drone_update):
        self.cursor.execute("insert into UITVOERING values (%s, %s, FROM_UNIXTIME(%s * 0.001), %s, %s, NULL, NULL, %s)",
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
        return self.cursor

    def empty(self):
        self.cursor.execute("delete from UITVOERING")
        return

    def count_records(self):
        self.cursor.execute("select count(*) from UITVOERING")
        return self.cursor.fetchone()[0]


class MongoDB(Database):

    def __init__(self):
        self.client = None
        self.db = None
        self.uitvoering_col = None

    def connect(self, ipv4, database_name):
        self.client = MongoClient(ipv4, 27017)
        self.db = self.client[database_name]
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
        return self.uitvoering_col.find().limit(aantal_records)

    def empty(self):
        self.uitvoering_col.drop()
        return

    def count_records(self):
        return self.uitvoering_col.count()

