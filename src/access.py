import os, sqlalchemy
import os.path
import pandas as pd
import numpy as np
from src.utils.utils import db, sql_str, config, Acc, Ingester
from sqlalchemy import create_engine, DDL
import sqlalchemy_access as sa_a
from psycopg2 import sql
from tqdm import tqdm
from datetime import date
import urllib
import pyodbc as pyo


# all that is needed is header_build to build dataframe (per table)
# type_lookup to match datacolumn definitions
# send_to_pg to send to DB


def revised_otf(df, otftypes):
    for i in df.columns:
        if i not in otftypes.keys():
            if df.dtypes[i]=='object':
                otftypes[i] = sqlalchemy.sql.sqltypes.String
            elif df.dtypes[i]=="int64":
                otftypes[i] = sqlalchemy.types.Integer
            elif df.dtypes[i]=="float64":
                otftypes[i] = sqlalchemy.sql.sqltypes.Float
        else:
            if df.dtypes[i]=="int64" or df.dtypes[i]=="Int64":
                otftypes[i] = sqlalchemy.types.Integer
            else:
                pass
    return otftypes

def header_build(nri_update_path,tablename):
    tablepack = {}
    tablesets = [i for i in os.listdir(nri_update_path) if (not i.endswith(".accdb") and not i.endswith(".laccdb")) and ("Coordinates" not in i)]

    tbl_years = {
        'pasture2013-2018':2013,
        'range2011-2016':2011,
        'RangeChange2004-2008':2004,
        'RangeChange2009-2015':2009,
        'rangepasture2017_2018':2017,
        'pasture2019':2019,
        'pasture2020':2019,
        'range2019':2019,
        'range2020':2019,
    }
    try:
        count=0
        for i in tablesets:
            print(f"working on tableset '{i}'...")
            basepath = nri_update_path # ingestables/nriupdate
            path = os.path.join(nri_update_path,i)
            basebase = os.path.dirname(basepath) # ingestables/

            tblyear = tbl_years[i]
            cols = type_lookup(os.path.dirname(basebase),tablename, "types", tblyear)
            # print(path)
            if tablename in [i.split('.')[0] for i in os.listdir(path)]:
                print(f"working on {tablename}..")
                tempdf = pd.read_csv(
                    os.path.join(path,f'{tablename}.txt'),
                    sep='|',
                    index_col=False,
                    names=cols.keys(),
                    low_memory=False)

                if "statenm" in tablename:
                    print("fixing statenm..")
                    tempdf = state_fix(tempdf)

                if "pastureheights" in tablename:
                    print("fixing pastureheights...")
                    tempdf = ph_fix(tempdf)
                if "disturbance" in tablename:
                    print("fixing disturbance...")
                    tempdf = disturbance_fix(tempdf)
                if "practice" in tablename:
                    print("fixing practice ...")
                    tempdf = practice_fix(tempdf)

                if "point" in tablename:
                    print("fixing point ...")
                    tempdf = point_fix(tempdf)

                if "concern" in tablename:
                    print("fixing concern ...")
                    tempdf = concern_fix(tempdf)


                # print("checkpoint 2")
                # print("after 4 table fixes")
                fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']

                # if "pintercept" in tablename:
                # for i in tempdf.columns:
                #     if tempdf[i].dtype=="object":
                #         tempdf[i] = tempdf[i].apply(lambda x: pd.NA if type(x)!="float" and
                #                     pd.isnull(x)!=True and
                #                     (str(x).strip()=="." or
                #                     str(x).strip()=="")
                #                     else str(x).strip())

                if 'COUNTY' in tempdf.columns:
                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

                if 'STATE' in tempdf.columns:
                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')


                less_fields = ['statenm','countynm']
                if tablename not in less_fields:
                    # print("checkpoint 5")
                    tempdf = dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                    tempdf = dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])


                # print("checkpoint 6")

                # for i in tempdf.columns:
                #     if tempdf[i].dtype=='object':
                #         tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)!=True and
                #                 # type(x)!='float' and
                #                 # type(x)!='int' and
                #                 str(x).strip()==''
                #                 else x)
                tablepack[f'{tablename}_{count}'] = tempdf

                count+=1
            else:
                # print(f'{tablename} not in {i}')
                pass
        final = pd.concat([i for i in tablepack.values()])

        if "statenm" in tablename:
            final = statenm_fix(final)

        return final
            # return tempdf
    except Exception as e:
        print(e)

def tbl_choice(tablename, dir):
    basepath = os.path.normpath(r"C:\Users\kbonefont\Documents\GitHub\ingest_nri\ingestables\nriupdate")
    # basepath = p
     # ingestables/nriupdate
    paths = [i for i in os.listdir(basepath) if (".acc" not in i) and ("Point" not in i)]
    path = os.path.join(basepath,paths[dir],f"{tablename}.txt")

    basebase = os.path.dirname(basepath)
    cols = type_lookup(basebase, tablename.upper(), "types")

    return pd.read_csv(path, sep='|', index_col=False, names=cols.keys(), low_memory=False)

def state_fix(statenm):
    print("making copy")
    tempdf = statenm.copy(deep=True)
    midwest = ['IL', 'IN','IA','MI','MN','MO','OH','WI']
    northeast = ['CT', 'DE', 'ME', 'MD', 'MA', 'NH', 'NJ', 'NY', 'PA', 'RI', 'VT', 'WV']
    nplains = ['CO','KS', 'MT', 'NE', 'ND','SD','WY']
    scentral = ['AR','LA','OK','TX']
    seast = ['AL','FL','GA','KY', 'MS','NC','SC','TN','VA']
    west = ['AZ', 'CA', 'ID', 'NV', 'NM', 'OR','UT','WA']

    pastureids = {
        "Midwest":1,
        "North East": 2,
        "Northern Plains":3,
        "South Central": 4,
        "South East": 5,
        "West":6
    }

    def region_chooser(state):
        if state in midwest:
            return 'Midwest'
        elif state in northeast:
            return 'North East'
        elif state in nplains:
            return 'Northern Plains'
        elif state in scentral:
            return 'South Central'
        elif state in seast:
            return 'South East'
        elif state in west:
            return 'West'

    def id_chooser(region):
        if type(region)==str:
            return pastureids[region]
        else:
            return 0
    print("adding pastureregionname")
    tempdf['PastureRegionName'] = tempdf.STABBR.apply(lambda x: region_chooser(x))
    print("adding pastureregionid")
    tempdf['PastureRegionID'] = tempdf.PastureRegionName.apply(lambda x: id_chooser(x))
    return tempdf

def ph_fix(pastureheights):
    dfcopy = pastureheights.copy(deep=True)

    # fixes all values in inches except
    dfcopy.WHEIGHT = dfcopy.WHEIGHT.apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and
                    (any([y.isdigit() for y in x])==True) and
                    (any(['+' in z for z in x])!=True) and
                    ('in' in x.split()) else
                        (round(float(x.split()[0]),3) if pd.isnull(x)!=True and
                        (any([y.isdigit() for y in x])==True) and
                        (any(['+' in z for z in x])!=True) and
                        ('ft' in x.split()) else x) )
    # fixes all strings with spaces in values for WHEIGHT (no digits)
    dfcopy.WHEIGHT = dfcopy.WHEIGHT.apply(lambda x: np.nan if (type(x)!=float) and (type(x)==str) and ('0' not in x.strip()) else x )

    dfcopy.WHEIGHT = dfcopy.WHEIGHT.apply(lambda x: float(0) if (type(x)!=float) and (type(x)==str) and ('0' in x.strip()) else x )
    # replaces '61+ ft  ' with 61.0
    dfcopy.WHEIGHT = dfcopy.WHEIGHT.apply(lambda x: float(61) if (type(x)!=float) and (type(x)==str) and ("61+ ft" in x.strip()) else x)

    dfcopy.HEIGHT = dfcopy.HEIGHT.apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and
                    (any([y.isdigit() for y in x])==True) and
                    (any(['+' in z for z in x])!=True) and
                    ('in' in x.split()) else (round(float(x.split()[0]),3) if pd.isnull(x)!=True and
                        (any([y.isdigit() for y in x])==True) and
                        (any(['+' in z for z in x])!=True) and
                        ('ft' in x.split()) else x) )
    dfcopy.HEIGHT = dfcopy.HEIGHT.apply(lambda x: np.nan if (type(x)!=float) and (type(x)==str) and ('0' not in x.strip()) else x )

    dfcopy.HEIGHT = dfcopy.HEIGHT.apply(lambda x: float(0) if (type(x)!=float) and (type(x)==str) and ('0' in x.strip()) else x )
    # replaces '61+ ft  ' with 61.0
    dfcopy.HEIGHT = dfcopy.HEIGHT.apply(lambda x: float(61) if (type(x)!=float) and (type(x)==str) and ("61+ ft" in x.strip()) else x)

    dfcopy["HEIGHT_UNIT"] = 'ft'
    dfcopy["WHEIGHT_UNIT"] = 'ft'
    dfcopy = dfcopy.iloc[:,[0,1,2,3,4,5,6,7,8,11,9,10,12]]
    return dfcopy

def statenm_fix(statenm):
    dfcopy = statenm.copy(deep=True)
    noname = ['23', '25', '34', '15']
    if "STATENM" in dfcopy.columns:
        dfcopy_nonull = dfcopy[pd.isnull(dfcopy.STATENM)!=True].drop_duplicates()
    small = dfcopy[dfcopy.STATE.isin(noname) ].drop_duplicates()
    final = pd.concat([dfcopy_nonull,small])
    return final

def disturbance_fix(disturbance):
    dftemp = disturbance.copy(deep=True)
    for i in dftemp.columns[6:41]:
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: np.nan if (type(x)!=int) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].astype('Int64')
    return dftemp

def concern_fix(disturbance):
    dftemp = disturbance.copy(deep=True)
    for i in dftemp.columns[5:27]:
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: np.nan if (type(x)!=int) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].astype('Int64')
    return dftemp

def practice_fix(practice):
    dftemp = practice.copy(deep=True)
    for i in dftemp.columns[5:51]:
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: np.nan if (type(x)!=int) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].astype('Int64')

    for i in dftemp.columns[5:49]:
        if dftemp[f'{i}'].dtype=='object':
            dftemp[f'{i}'] = dftemp[f'{i}'].astype('Int64')

    if 'P528A' in dftemp.columns:
        dftemp.drop(columns=['P528A'], inplace=True)
    if 'N528A' in dftemp.columns:
        dftemp.drop(columns=['N528A'], inplace=True)
    return dftemp

def point_fix(point):
    yn_indices = ['FULL_ESD_PLOT', 'BASAL_GAPS_NESW', 'CANOPY_GAPS_NESW',
       'BASAL_GAPS_NWSE', 'CANOPY_GAPS_NWSE', 'HAYED', 'OPT_SSI_PASTURE',
       'OPT_DWR_PASTURE', 'OPT_DWR_RANGE', 'SAGE_EXISTS',
       'PERENNIAL_CANOPY_GAPS_NESW', 'PERENNIAL_CANOPY_GAPS_NWSE',
       'COMPONENT_POPULATED', 'GAPS_DIFFERENT_NESW', 'GAPS_DIFFERENT_NWSE']
    dftemp = point.copy(deep=True)
    for i in dftemp.filter(yn_indices):
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: np.nan if (type(x)!=int) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].astype('Int64')
    return dftemp

def ret_access(whichmdb):
    MDB = whichmdb
    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
    mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
    connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(mdb_string)}"
    engine = create_engine(connection_url)
    return engine

def type_lookup(basepath, tablename, which_return, year=None):
    """
    ex:
    type_lookup(path, "ALTWOODY", "lengths")
    """
    typelist = {}
    typelength = {}
    return_dict = {}

    columnexps = {
        "2004" : r"C:\Users\kbonefont\OneDrive - USDA\Documents\GitHub\ingest_nri\ingestables\2004-2008 NRI Range Change Data Dump Columns.xlsx",
        "2009" : r"C:\Users\kbonefont\OneDrive - USDA\Documents\GitHub\ingest_nri\ingestables\2009-2018 NRI Range Data Dump Columns.xlsx",
        "2019" : r"C:\Users\kbonefont\OneDrive - USDA\Documents\GitHub\ingest_nri\ingestables\nri_data_column_explanations.csv"
    }

    if int(year)>=2004 and int(year)<=2008:
        exp_file = columnexps["2004"]
    elif int(year)>=2009 and int(year)<=2018:
        exp_file = columnexps["2009"]
    else:
        exp_file = columnexps["2019"]


    nri_explanations = pd.read_excel(exp_file) if (int(year)>=2004) and (int(year)<=2018) else pd.read_csv(exp_file)
    is_target = nri_explanations['Table name'] == f'{tablename.upper()}'
    original_list = nri_explanations[is_target]['Field name'].values

    for i in original_list:
        temprow = nri_explanations[is_target].copy()

        packed = temprow["Data type"].values
        lengths = temprow["Field size"].values

        typelist[i]=packed[original_list.tolist().index(i)]
        typelength[i]=lengths[original_list.tolist().index(i)]

    return_dict.update({"types":typelist})
    return_dict.update({"lengths":typelength})

    return return_dict[which_return]

def batcher(nriupdate_path, acc_path, mdb=True):
    tables = {}
    tablelist = table_list_creator(nriupdate_path)
    # collect all tables
    for i in tablelist:
        df = header_build(nriupdate_path, i)
        tables[i] = df
    # send all tables
    for k,v in tables.items():
        pg_send(v,acc_path,k, access=True if mdb==True else False)

def table_list_creator(filedirectory):
    table_master_set = set()
    nriupdate_dirs = [i for i in os.listdir(filedirectory) if (not i.endswith(".accdb") and not i.endswith(".laccdb")) and ("Coordinates" not in i)]
    for i in nriupdate_dirs:
        # print(i)
        for j in [k.split('.')[0] for k in os.listdir(os.path.join(filedirectory,i))]:
            table_master_set.add(j)
    return [i for i in table_master_set]

def pg_send(df,acc_path, tablename, access = False):
    """
    usage:
    pg_send('target_directory_path', 'path_to_access_file', target_dictionary, 'target_table', acces=False/True, pg=False/True)
    todo:
    X switching off/on access and pg --done
    """
    d =db("nri")
    con = d.str
    cursor = con.cursor()
    #access path changes!
    cxn = ret_access(acc_path)

    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    engine = create_engine(sql_str(config()))

    chunksize = int(len(df) / 10)
    tqdm.write(f'sending {tablename} to pg...')
    only_once = set()
    onthefly = {}
    basebase = os.path.dirname(acc_path)
    if access!=False:
        with tqdm(total=len(df)) as pbar:
            """
            loop to use the progress bar on each iteration
            """
            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"


                onthefly = revised_otf(df, onthefly)



                # if access!=False:
                cdf.to_sql(name=f'{tablename}',
                           con=ret_access(acc_path),
                           index=False,
                           dtype=onthefly,
                           if_exists=replace)
                # else clause will send to pg
                # else:
                #     Ingester.main_ingest(cdf)



                pbar.update(chunksize)
            tqdm._instances.clear()
        tqdm.write(f'{tablename} sent to db')
    else:
        try:
            if tablecheck(tablename):
                Ingester.main_ingest(df, tablename, con)
            else:
                table_create(df, tablename)
                Ingester.main_ingest(df, tablename, con)

        except Exception as e:
            print(e)

def dbkey_gen(df,newfield, *fields):
    dfcopy = df.copy()
    dfcopy[f'{newfield}'] = (df[[f'{field.strip()}' for field in fields]].astype(str)).agg(''.join,axis=1).astype(object)
    return dfcopy


def table_create(df:pd.DataFrame, tablename: str):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """
    d = db('nri')

    try:
        print("checking fields")
        comm = create_command(df, tablename)
        con = d.str
        cur = con.cursor()
        # return comm
        cur.execute(comm)
        # cur.execute(tables.set_srid()) if "Header" in tablename else None
        con.commit()

    except Exception as e:
        print(e)
        d = db('nri')
        con = d.str
        cur = con.cursor()



def create_command(df, tablename):
    """
    creates a complete CREATE TABLE postgres statement
    by only supplying tablename
    currently handles: HEADER, Gap
    """

    str = f'CREATE TABLE "{tablename}" '
    str+=field_appender(df)


    return str


def df2pg(df):
    type_translate = {
        np.dtype('int64'): "INTEGER",
        pd.Int64Dtype(): "INTEGER",
        np.dtype('O'): "TEXT",
        np.dtype("float64"): "NUMERIC",
        np.dtype("datetime64"): "DATE"
    }
    return {i:type_translate[j] for i,j in df.dtypes.to_dict().items()}


def field_appender(df):
    """
    uses schema_chooser to pull a schema for a specific table
    and create a postgres-ready string of fields and field types
    """

    str = "( "
    count = 0
    di = df2pg(df)

    for k,v in di.items():
        if count<(len(di.items())-1):
            str+= f'"{k}" {v.upper()}, '
            count+=1
        else:
            str+= f'"{k}" {v.upper()} );'
    return str



def tablecheck(tablename):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    tableschema = 'public'
    try:
        d = db('nri')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = db('nri')
        con = d.str
        cur = con.cursor()
