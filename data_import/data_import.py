import os
import sys
from dotenv import load_dotenv
import csv
import jaydebeapi
import shutil
import random

load_dotenv()


class DataFile:
    remote_filepath = os.getenv("PSS_PATH")
    local_server = os.getenv("SKORUZ_IP")
    local_filepath = os.getenv("SKORUZ_PATH")
    remote_server = os.getenv("SKORUZ_WEBSITE")
    server_port = os.getenv("SKORUZ_PORT")
    server_database = os.getenv("SKORUZ_DB")

    def load_data(self):
        file = sys.argv[1]
        filename = os.path.splitext(file)[0]
        filetype = os.path.splitext(file)[1]
        data_dir = os.path.join(os.getcwd(), "datafile/" + file)
        data = []
        header = []

        if filetype == ".tsv":
            with open(data_dir) as tsvfile:
                reader = csv.reader(tsvfile, delimiter='\t')
                for row in reader:
                    data.append(row)
            length = len(data[0])
            j = 0
            for i in data[0]:
                header.append(i)
                header.append('STRING')
                if j < length - 1:
                    header.append(";")
                j += 1
            header = ",".join(header)
            header = header.replace(',', ' ')
            header = header.replace(';', ',')
            self.create_table(file, filename, filetype, header, data_dir)

        elif filetype == ".csv":
            with open(data_dir) as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                for row in reader:
                    data.append(row)
            length = len(data[0])
            j = 0
            for i in data[0]:
                header.append(i)
                header.append('STRING')
                if j < length - 1:
                    header.append(";")
                j += 1
            header = ",".join(header)
            header = header.replace(',', ' ')
            header = header.replace(';', ',')
            self.create_table(file, filename, filetype, header, data_dir)

        else:
            data_dir = os.path.join(os.getcwd(), "datafile/" + file)
            f = open(data_dir, "r")
            datastore = f.read()
            try:
                conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver",
                                          "jdbc:hive2://" + self.local_server + ":" + self.server_port +
                                          "/" + self.server_database, {'user': "hive", 'password': ""},
                                          self.local_filepath)
                curs = conn.cursor()
                curs.execute('CREATE TABLE ' + filename + ' (str STRING) STORED AS ORC')
                curs.execute('INSERT INTO ' + filename + ' VALUES (\'' + datastore + '\')')
                conn.close()

            except Exception:
                print("Error")

    def create_table(self, file, filename, filetype, header, data_dir):
        try:
            conn = jaydebeapi.connect("com.simba.hive.jdbc41.HS2Driver",
                                      "jdbc:hive2://" + self.local_server + ":" + self.server_port +
                                      "/" + self.server_database, {'user': "hive", 'password': ""},
                                      self.local_filepath)
            curs = conn.cursor()
            new_filename = filename + "_" + str(random.randrange(1, 1000, 3))
            curs.execute('CREATE TABLE ' + new_filename + '(' + header + ') STORED AS ORC')
            conn.close()
            new_path = os.path.join(os.getcwd(), "movedfiles/" + file)
            shutil.copy(data_dir, new_path)
            os.rename(new_path, os.path.join(os.getcwd(), "movedfiles/" + new_filename + filetype))
            print("Table Name: ", new_filename)

        except Exception:
            print("Error")


d1 = DataFile()
d1.load_data()