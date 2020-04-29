from Databases import *
from DroneData import *
import concurrent.futures
import threading
import time
import sys
import json


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


def write_test(db_instance, n_records, index, n_threads):
    """
    Methode die de write test initialiseert. Momenteel is dit nog een voorbeeld implementatie!!

    :param db_instance: het database object van de database die getest wordt
    :return: Niks
    """

    data = DroneData()

    multiplier = index * (int(n_records) / int(n_threads))

    # print("The amount of records in the before test database is: " +
    #       str(db_instance.count_records()))
    # print("Starting write test for " + str(n_records) + " records")

    for i in range(0, int(int(n_records) / int(n_threads))):
        data.new_update()
        db_instance.write(data, i + int(multiplier))

    return


def read_test(db_instance):
    """
    Methode die de read test initialiseert. Momenteel nog geen voorbeeld implementatie!!

    :param db_instance: het database object van de database die getest wordt
    :return:
    """
    print("Read test not implemented yet")
    return


def write_result(database_type, n_records, duration):
    result = {}
    result[database_type] = []
    result[database_type].append({
        n_records: duration
    })

    with open('result ' + str(database_type) + '.json', 'a') as outfile:
        json.dump(result, outfile)


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

    if len(sys.argv) is not 5:
        print("Must specify database name, test type and number of records: <database> <type> <number of records> <number of threads> <test case>")
        exit(1)

    database_type = sys.argv[1]
    test_type = sys.argv[2]
    n_records = sys.argv[3]
    n_threads = sys.argv[4]

    db_instance = get_database_instance(database_type)
    if db_instance is None:
        print("Unknown database type: " + database_type)
        exit(1)

    db_instance.connect("127.0.0.1", "db0")

    if test_type == "write":
        for test_case in range(1, 6):
            start_time = int(round(time.time() * 1000))
            for index in range(int(n_threads)):
                x = threading.Thread(target=write_test, args=(
                    db_instance, n_records, index, n_threads))
                x.start()
                x.join()
            end_time = int(round(time.time() * 1000))
            duration = end_time - start_time
            print("Total write time of test case number " +
                  str(test_case) + ': ' + str(duration) + "ms")
            db_instance.empty()
            write_result(database_type, n_records, duration)

    elif test_type == "read":
        read_test(db_instance)
    else:
        print("unknown test type: " + test_type)
        exit(1)

    exit(0)


main()
