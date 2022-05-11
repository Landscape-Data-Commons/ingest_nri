import pyodbc
import io
import psycopg2
import os

import pandas as pd
import psycopg2.pool as pgpool
import logging
from tqdm import tqdm

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






class Ingester:

    def __init__(self, con):
        """ clearing old instances """
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.__tablenames = []
        self.__seen = set()

        """ init connection objects """
        self.con = con
        self.cur = self.con.cursor()

    @staticmethod
    def main_ingest( df: pd.DataFrame, table:str,
                    connection: psycopg2.extensions.connection,
                    chunk_size:int = 10000):
        """needs a table first"""

        df = df.copy()
        # itll throw truth value is ambiguous if there are dup columns
        escaped = {'\\': '\\\\', '\n': r'\n', '\r': r'\r', '\t': r'\t',}
        for col in df.columns:
            if df.dtypes[col] == 'object':
                for v, e in escaped.items():
                    df[col] = df[col].apply(lambda x: x.replace(v, '') if (x is not None) and (isinstance(x,str)) else x)
        try:
            logging.info(f"{table}: {df.columns}")
            conn = connection
            cursor = conn.cursor()
            for i in tqdm(range(0, df.shape[0], chunk_size)):
                f = io.StringIO()
                chunk = df.iloc[i:(i + chunk_size)]

                chunk.to_csv(f, index=False, header=False, sep='\t', na_rep='\\N', quoting=None)
                f.seek(0)
                cursor.copy_from(f, f'"{table}"', columns=[f'"{i}"' for i in df.columns])
                connection.commit()
        except psycopg2.Error as e:
            print(e)
            conn = connection
            cursor = conn.cursor()
            conn.rollback()
        cursor.close()




def sql_str(args):
    str = os.path.join('postgresql://'+args['user']+':'+args['password']+'@'+args['host']+'/'+args['database'])
    return str
