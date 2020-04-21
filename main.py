from Databases import *
from DroneData import *
import time
import sys


def get_database_instance(db_name):
    """
    Methode om het correcte database object te instantiëren
    :param db_name: de naam van het type database
    :return: het database object, of None als het type database niet bestaat.
    """
    switcher = {
        "sql": SQL(),
        "mongodb": MongoDB(),
        "redis": Redis(),
        "hbase": Happybase(),
    }
    return switcher.get(db_name, None)


def write_test(db_instance):
    """
    Methode die de write test initialiseert. Momenteel is dit nog een voorbeeld implementatie!!

    :param db_instance: het database object van de database die getest wordt
    :return: Niks
    """

    data = DroneData()
    amount_records = 10
    print("The amount of records in the before test database is: " +
          str(db_instance.count_records()))
    print("Starting write test for " + str(amount_records) + " records")

    start_time = int(round(time.time() * 1000))

    for i in range(0, amount_records):
        data.new_update()
        db_instance.write(data, i)

    end_time = int(round(time.time() * 1000))
    duration = end_time - start_time
    print("Query runtime: " + str(duration) + "ms")
    print("The amount of records in the database is now: " +
          str(db_instance.count_records()))

    return


def read_test(db_instance):
    """
    Methode die de read test initialiseert. Momenteel nog geen voorbeeld implementatie!!

    :param db_instance: het database object van de database die getest wordt
    :return:
    """
    print("Read test not implemented yet")
    return


def main():
    """
    main methode. Verwacht dat via de command line de volgende argumenten zijn gegeven:
    <database_type> <test_type>.

    Met het eerste argument wordt een database object geïnstantieert. Aan de hand van het
    tweede argument wordt bepaald welke test (read of write) wordt verricht.

    de main methode zorgt dat de database connectie wordt opgezet en roept vervolgens de methode aan
    voor een van de twee tests.

    :return: niks
    """

    if len(sys.argv) is not 3:
        print("Must specify database name and test type: <database> <type>")
        exit(1)

    database_type = sys.argv[1]
    test_type = sys.argv[2]

    db_instance = get_database_instance(database_type)
    if db_instance is None:
        print("Unknown database type: " + database_type)
        exit(1)

    db_instance.connect("127.0.0.1", "db0")

    if test_type == "write":
        write_test(db_instance)
    elif test_type == "read":
        read_test(db_instance)
    else:
        print("unknown test type: " + test_type)
        exit(1)

    exit(0)


main()
