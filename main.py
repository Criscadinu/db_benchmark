from databases.Databases import *
from DroneData import *
import concurrent.futures
import threading
import time
import sys
import json
import os


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
        "monetdb": Monetdb(),
    }
    return switcher.get(db_name, None)


def write_test(database_type, db_instance, n_records, test_case, result_set, file_name):
    """
    Methode die de write test initialiseert. Momenteel is dit nog een voorbeeld implementatie!!
    :param db_instance: het database object van de database die getest wordt
    :return: Niks
    """

    data = DroneData()

    result = {}

    ###############HOLY BLOCK DONT TOUCH######################
    start_time = int(round(time.time() * 1000))

    for i in range(0, int(n_records)):
        data.new_update()
        db_instance.write(data)

    end_time = int(round(time.time() * 1000))
    ##########################################################

    duration = end_time - start_time
    print("Total write time of test case number " +
          str(test_case) + ': ' + str(duration) + "ms")
    print("Cleanup! Removing inserted records from database..")
#    db_instance.empty()
    result_set[test_case] = duration
    result[n_records] = []
    result[n_records].append(result_set)

    if test_case == 5:
        write_to_json_file(result, file_name)


    return


def read_test(database_type, db_instance, n_records, test_case, result_set, file_name):
#     """
#     Methode die de read test initialiseert. Momenteel nog geen voorbeeld implementatie!!

#     :param db_instance: het database object van de database die getest wordt
#     :return:
#     """

    result = {}

    ###############HOLY BLOCK DONT TOUCH######################
    start_time = datetime.datetime.now()
    db_instance.read(int(n_records))

    end_time = datetime.datetime.now()
    ##########################################################

    duration = end_time - start_time
    print("Total write time of test case number " +
          str(test_case) + ': ' + str(duration.microseconds) + "us")
    result_set[test_case] = duration.microseconds
    result[n_records] = []
    result[n_records].append(result_set)
    if test_case == 5:
        write_to_json_file(result, file_name)
    return


def is_json_file(file_name):
    return os.path.exists(file_name)


def create_json_file(file_name):
    if is_json_file(file_name):
        pass
    else:
        data = {}
        data['tests'] = []
        with open(file_name, 'w') as outfile:
            json.dump(data, outfile)


def write_to_json_file(result, filename):

    with open(filename) as json_file:
        data = json.load(json_file)
        temp = data["tests"]

        temp.append(result)

        with open(filename, 'w') as f:
            json.dump(data, f, indent=4)


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

    database_type = sys.argv[1]
    test_type = sys.argv[2]
    n_records = sys.argv[3]
    result_set = {}

    db_instance = get_database_instance(database_type)
    if db_instance is None:
        print("Unknown database type: " + database_type)
        exit(1)

    db_instance.connect("127.0.0.1")

    file_name = 'result_' + str(database_type) + '.json'
    read_file_name = 'read_result_' + str(database_type) + '.json'

    create_json_file(file_name)
    create_json_file(read_file_name)

    if test_type == "write":
        for test_case in range(1, 6):
            write_test(database_type, db_instance,
                       n_records, test_case, result_set, file_name)
    elif test_type == "read":
        for test_case in range(1, 6):
            read_test(database_type, db_instance,
                      n_records, test_case, result_set, read_file_name)
    else:
        print("unknown test type: " + test_type)
        exit(1)

    exit(0)


if len(sys.argv) is not 4:
    print("Must specify database name, test type and number of records: <database> <type> <number of records>")
    exit(1)

main()
