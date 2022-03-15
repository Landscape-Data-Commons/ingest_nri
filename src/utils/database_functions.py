import pyodbc
from psycopg2 import connect, sql
import pandas as pd
from os import chdir, getcwd
from os.path import abspath, join
from configparser import ConfigParser
from psycopg2.pool import SimpleConnectionPool
import jaydebeapi
import platform
import pyodbc
import platform
p = r"C:\Users\kbonefont\Desktop\kris\NRI\extracted\2017-2018 Range and Pasture\Raw data dump\rangepasture17_no_pintercept.accdb"

arcno.MakeTableView("point", p).LANDUSE.unique()

class arcno():
    __maintablelist = [
     "altwoody",
     "biomass",
     "concern",
     "condition",
     "countynm",
     "disturbance",
     "dryweightrank",
     "esfsg",
     "gintercept",
     "gps",
     "pastureheights",
     "pintercept",
     "plantcensus",
     "point",
     "pointcoordinates",
     "practice",
     "ptnote",
     "relativeyields",
     "soildisag",
     "soilhorizon",
     "statenm"
      ]
    correct = {
        "ALTWOODY":"altwoody",
        "BIOMASS":"biomass",
        "CONCERN":"concern",
        "CONDITION":"condition",
        "COUNTYNM":"countynm",
        "DISTURBANCE":"disturbance",
        "DRYWEIGHTRANK":"dryweightrank",
        "ESFSG":"esfsg",
        "GINTERCEPT":"gintercept",
        "GPS":"gps",
        "PASTUREHEIGHTS":"pastureheights",
        "PINTERCEPT":"pintercept",
        "PLANTCENSUS":"plantcensus",
        "POINT":"point",
        "POINTCOORDINATES":"pointcoordinates",
        "PRACTICE":"practice",
        "PTNOTE":"ptnote",
        "RELATIVEYIELDS":"relativeyields",
        "SOILDISAG":"soildisag",
        "SOILHORIZON":"soilhorizon",
        "STATENM":"statenm"
        }

    # tablelist = []
    # actual_list = {}
    isolates = None

    def __init__(self, whichdima = None, all=False):
        """ Initializes a list of tables in dima accessible on tablelist.
        ex.
        arc = arcno(path_to_dima)
        arc.tablelist
        """
        [self.clear(a) for a in dir(self) if not a.startswith('__') and not callable(getattr(self,a))]
        self.whichdima = whichdima
        self.tablelist=[]
        if self.whichdima is not None:
            conn = Acc(f"{self.whichdima}").con if platform.system()=='Windows' else Acc2(f"{self.whichdima}").con
            # conn.setdecoding(pyodbc.SQL_CHAR, encoding="cp1252")
            # conn.setdecoding(pyodbc.SQL_WCHAR, encoding="cp1252")
            conn.setencoding(encoding='utf-16le') if platform.system()=='Windows' else None
            # conn.setdecoding(pyodbc.SQL_WMETADATA, encoding='utf-8')
            # conn.setdecoding(pyodbc.SQL_FLOAT, encoding='utf-8')
            # conn.setdecoding(pyodbc.SQL_DOUBLE, encoding='utf-8')
            if platform.system()=='Windows':
                cursor = conn.cursor()
                for t in cursor.tables():
                    if t.table_name.startswith('tbl'):
                        self.tablelist.append(t.table_name)
            else:
                cursor = conn.cursor()
                qry = r"SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'TBL%'"
                cursor.execute(qry)
                trick = [i[0] for i in cursor.fetchall()]
                self.tablelist = [self.correct[i] for i in trick if i in self.correct.keys()]



        self.actual_list = {}
        for i in self.tablelist:
            if all!=True:
                # print('false')
                if (self.MakeTableView(i,whichdima).shape[0]>=1) and (i in self.__maintablelist):
                    self.actual_list.update({i:f'rows: {self.MakeTableView(i,whichdima).shape[0]}'})
            else:
                # print('true')
                if self.MakeTableView(i,whichdima).shape[0]>=1:
                    self.actual_list.update({i:f'rows: {self.MakeTableView(i,whichdima).shape[0]}'})

    def clear(self,var):
        if isinstance(var, list):
            var = []
            return var
        elif isinstance(var, dict):
            var = {}
            return var
        else:
            var = None
            return var


    @staticmethod
    def MakeTableView(in_table,whichdima):
        """ connects to Microsoft Access .mdb file, selects a table
        and copies it to a dataframe.
        ex.
        arc = arcno()
        arc.MakeTableView('table_name', 'dima_path')

        """
        # self.in_table = in_table
        # self.whichdima = whichdima

        try:
            return Table(in_table, whichdima).temp
        except Exception as e:
            print(e)

    def SelectLayerByAttribute(self, in_df,*vals, field = None):

        import pandas as pd
        self.in_df = in_df
        self.field = field
        self.vals = vals

        dfset = []
        def name(arg1,arg2):
            self.arg1 = arg1
            self.arg2 = arg2
            import os
            joined= os.path.join(self.arg1+self.arg2)
            return joined

        if all(self.in_df):
            print("dataframe exists")
            try:
                for val in self.vals:
                    index = self.in_df[f'{self.field}']==f'{val}'
                    exec("%s = self.in_df[index]" % name(f'{self.field}',f'{val}'))
                    dfset.append(eval(name(f'{self.field}',f'{val}')))

                return pd.concat(dfset)
            except Exception as e:
                print(e)
        else:
            print("error")
    def GetCount(self,in_df):
        """ Returns number of rows in dataframe
        """
        self.in_df = in_df
        return self.in_df.shape[0]

    @staticmethod
    def AddJoin(in_df, df2, right_on=None, left_on=None):
        """ inner join on two dataframes on 1 or 2 fields
        ex.
        arc = arcno()
        arc.AddJoin('dataframe_x', 'dataframe_y', 'field_a')
        """
        # self.temp_table = None
        d={}
        right_on = None
        left_on = None

        d[right_on] = right_on
        d[left_on] = left_on

        # self.in_df = in_df
        # self.df2 = df2

        if right_on==left_on and len(in_df.columns)==len(df2.columns):
            try:
                frames = [in_df, df2]
                return pd.concat(frames)
            except Exception as e:
                print(e)
                print('1. field or fields invalid' )
        elif right_on == left_on and len(in_df.columns) != len(df2.columns):
            try:
                # frames = [self.in_df, self.df2]
                return in_df.merge(df2, on = d[right_on], how='inner')
            except Exception as e:
                print(e)
                print('2. field or fields invalid')
        else:
            try:
                return in_df.merge(df2,right_on=d[right_on], left_on=d[left_on])
            except Exception as e:
                print(e)
                print('3. field or fields invalid')


    def CalculateField(self,in_df,newfield,*fields):
        """ Creates a newfield by concatenating any number of existing fields
        ex.
        arc = arcno()
        arc.CalculateField('dataframe_x', 'name_of_new_field', 'field_x', 'field_y','field_z')
        field_x = 'red'
        field_y = 'blue'
        field_z = 'green'
        name_of_new_field = 'redbluegreen'
        """
        self.in_df = in_df
        self.newfield = newfield
        self.fields = fields

        self.in_df[f'{self.newfield}'] = (self.in_df[[f'{field}' for field in self.fields]].astype(str)).sum(axis=1)
        return self.in_df



    def AddField(self, in_df, newfield):
        """ adds empty field within 'in_df' with fieldname
        supplied in the argument
        """
        self.in_df = in_df
        self.newfield = newfield

        self.in_df[f'{self.newfield}'] = pd.Series()
        return self.in_df

    def RemoveJoin(self):
        """ creates deep copy of original dataset
        and joins any new fields product of previous
        right hand join
        """
        pass

    def isolateFields(self,in_df,*fields):
        """ creates a new dataframe with submitted
        fields.
        """
        self.in_df = in_df
        self.fields = fields
        self.isolates =self.in_df[[f'{field}' for field in self.fields]]
        return self.isolates


    def GetParameterAsText(self,string):
        """ return stringified element
        """
        self.string = f'{string}'
        return self.string


class Table:
    temp=None
    def __init__(self,in_table=None, path=None):
        self.path = path
        self.in_table = in_table
        con = Acc(self.path).con if platform.system()=='Windows' else Acc2(f"{self.path}").con

        con.setencoding(encoding='utf-16le') if platform.system()=='Windows' else None

        query = f'SELECT * FROM "{self.in_table}"' if platform.system()=='Windows' else f'SELECT * FROM {self.in_table}'
        self.temp = pd.read_sql(query,con)

    def temp(self):
        return self.temp



















"""
EXTRACTING TABLES FROM ACCESS .MDB FILE INSIDE A CONTAINER (NOT WINDOWS)
"""

ucanaccess_jars = [
r"/usr/src/src/utils/UCanAccess-5.0.0-bin/ucanaccess-5.0.0.jar",
r"/usr/src/src/utils/UCanAccess-5.0.0-bin/lib/commons-lang3-3.8.1.jar",
r"/usr/src/src/utils/UCanAccess-5.0.0-bin/lib/commons-logging-1.2.jar",
r"/usr/src/src/utils/UCanAccess-5.0.0-bin/lib/hsqldb-2.5.0.jar",
r"/usr/src/src/utils/UCanAccess-5.0.0-bin/lib/jackcess-3.0.1.jar"]

classpath = ":".join(ucanaccess_jars)

def jdbc_path(path):
    return f'jdbc:ucanaccess://"{path}";newDatabaseVersion=V2010'

def jaycon(path):
    con = jaydebeapi.connect(drv_str1, jdbc_path(path), ["",""],classpath)
    return con

class Acc2:
    con=None
    def __init__(self, whichdima):
        self.whichdima=whichdima
        MDB = self.whichdima
        drv_str1 = "net.ucanaccess.jdbc.UcanaccessDriver"
        DRV = '{/out/lib/libmdbodbc.so}' if platform.system()=='Linux' else '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = f'jdbc:ucanaccess://{MDB};newDatabaseVersion=V2010'
        # print(mdb_string)
        self.con = jaydebeapi.connect(drv_str1, mdb_string,["",""],classpath)
        # self.con.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        # self.con.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')

    def db(self):
        try:
            return self.con
        except Exception as e:
            print(e)

"""
EXTRACTING DATA FROM .MDB FILE USING WINDOWS
"""
class Acc:
    con=None
    def __init__(self, whichdima):
        self.whichdima=whichdima
        MDB = self.whichdima
        # DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
        DRV = '{/out/lib/libmdbodbc.so}' if platform.system()=='Linux' else '{Microsoft Access Driver (*.mdb, *.accdb)}'
        mdb_string = f"DRIVER={DRV};DBQ=\"{MDB}\";" if platform.system()=='Linux' else f"DRIVER={DRV};DBQ={MDB};"
        # print(mdb_string)
        self.con = pyodbc.connect(mdb_string)
        # self.con.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
        # self.con.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')

    def db(self):
        try:
            return self.con
        except Exception as e:
            print(e)


def config(filename='src/utils/database.ini', section='postgresql'):
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

def dimaconfig(filename='src/utils/database.ini', section='dima'):
    """
    Same as config but reads another section.
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
    def __init__(self, keyword = None):
        if keyword == None:
            self.params = config()
            self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
            self.str = self.str_1.getconn()
        else:
            if "dimadev" in keyword:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=dimadev"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()


            else:
                self.params = config(section=f'{keyword}')
                self.params['options'] = "-c search_path=public"
                self.str_1 = SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
                self.str = self.str_1.getconn()


def searchpath_test(keyword):
    d = db(keyword)

    try:
        con = d.str
        cur = con.cursor()
        sql = 'show search_path'
        cur.execute(sql)
        print(cur.fetchone())
    except Exception as e:
        print(e)
        con = d.str
        cur = con.cursor()
