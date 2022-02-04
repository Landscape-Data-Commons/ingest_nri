import pyodbc
from psycopg2 import connect, sql
from os import chdir, getcwd
from os.path import abspath, join
from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool

class Acc:
    con=None

    def __init__(self, whichdima):
        self.whichdima=whichdima
        MDB = self.whichdima
        DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
        self.con = pyodbc.connect(mdb_string)

    def db(self):
        try:
            return self.con
        except Exception as e:
            print(e)



def config(filename='src/utils/database.ini', section='nri'):
    """
    Uses the configpaser module to read .ini and return a dictionary of
    credentials
    """
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(
        section, filename))

    return db

class db:
    params = config()
    # str = connect(**params)
    str_1 = SimpleConnectionPool(minconn=1,maxconn=5,**params)
    str = str_1.getconn()

    def __init__(self):

        self._conn = connect(**params)
        self._cur= self._conn.cursor()





def sql_str(args):
    str = join('postgresql://'+args['user']+':'+args['password']+'@'+args['host']+'/'+args['database'])
    return str
