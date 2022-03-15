import os, sqlalchemy
import os.path
import pandas as pd
import numpy as np
from src.utils.utils import db, sql_str, config, Acc
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

# todo: on the fly dtypes on send_to_pg, double check previous col fixes on older datasets

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

    tempdf['PastureRegionName'] = tempdf.STABBR.apply(lambda x: region_chooser(x))
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

def disturbance_fix(disturbance):
    dftemp = disturbance.copy(deep=True)
    for i in dftemp.columns[6:41]:
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
    return dftemp


def practice_fix(practice):
    dftemp = practice.copy(deep=True)
    for i in dftemp.columns[5:51]:
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 1 if (type(x)==str) and ("Y" in x) else x)
        dftemp[f'{i}'] = dftemp[f'{i}'].apply(lambda x: 0 if (type(x)==str) and ("N" in x) else x)
    dftemp.drop(columns=['P528A', 'N528A'], inplace=True)
    return dftemp


def ret_access(whichmdb):
    MDB = whichmdb
    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
    mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
    connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(mdb_string)}"
    engine = create_engine(connection_url)
    return engine

def type_lookup(basepath, tablename, which_return):
    """
    ex:
    type_lookup(path, "ALTWOODY", "lengths")
    """
    typelist = {}
    typelength = {}
    return_dict = {}

    where_expl = [ i for i in os.listdir(basepath) if 'explanations' in i]
    exp_file = os.path.join(basepath,where_expl[0])
    nri_explanations = pd.read_csv(exp_file)
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

def header_build(nri_update_path,tablename):
    tablepack = {}
    tablesets = [i for i in os.listdir(nri_update_path) if (not i.endswith(".accdb") and not i.endswith(".laccdb")) and ("Coordinates" not in i)]
    try:
        count=0
        for i in tablesets:
            basepath = nri_update_path # ingestables/nriupdate
            path = os.path.join(nri_update_path,i)
            basebase = os.path.dirname(basepath) # ingestables/

            cols = type_lookup(basebase, tablename.upper(), "types")
            # print(path)
            if tablename in [i.split('.')[0] for i in os.listdir(path)]:
                # print("yes")
                tempdf = pd.read_csv(os.path.join(path,f'{tablename}.txt'), sep='|', index_col=False, names=cols.keys(), low_memory=False)

                if "statenm" in tablename:
                    tempdf = state_fix(tempdf)

                if "pastureheights" in tablename:
                    print("checkpoint 1")
                    tempdf = ph_fix(tempdf)
                if "disturbance" in tablename:
                    tempdf = disturbance_fix(tempdf)
                if "practice" in tablename:
                    tempdf = practice_fix(tempdf)

                print("checkpoint 2")
                fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
                for field in tempdf.columns:
                    # first fixes
                    if 'SAGEBRUSH_SHAPE' in tempdf.columns and tempdf.SAGEBRUSH_SHAPE.dtype==object:
                        tempdf['SAGEBRUSH_SHAPE'] = tempdf['SAGEBRUSH_SHAPE'].apply(lambda x: np.nan if (' ' in x) else x).astype('float').astype('Int64')

                    # if (cols[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64) and (tempdf[field].dtype!='int') and (tempdf[field].dtype!='Int64'):
                    #     tempdf[field] = tempdf[field].apply(lambda i: i.strip())
                    #     tempdf[field] = pd.to_numeric(tempdf[field])
                print("checkpoint 3")
                if 'COUNTY' in tempdf.columns:
                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

                if 'STATE' in tempdf.columns:
                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')

                # if 'SAGEBRUSH_SHAPE' in tempdf.columns and tempdf.SAGEBRUSH_SHAPE.dtype==object:
                #     tempdf['SAGEBRUSH_SHAPE'] = tempdf['SAGEBRUSH_SHAPE'].apply(lambda x: np.nan if (' ' in x) else x).astype('float').astype('Int64')

                print("checkpoint 4")
                dot_list = ['HIT1','HIT2','HIT3', 'HIT4', 'HIT5', 'HIT6', 'NONSOIL']
                if field in dot_list:
                    tempdf[field] = tempdf[field].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)

                                        ##### STRIP ANYWAY
                if tempdf[field].dtype==np.object:
                    print("second strip")
                    tempdf[field] = tempdf[field].apply(lambda i: i.strip() if (type(i)!=float) and (type(i)!=int) else i)


                less_fields = ['statenm','countynm']
                if tablename not in less_fields:
                    print("checkpoint 5")
                    tempdf = dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                    tempdf = dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])


                print("checkpoint 6")
                tablepack[f'{tablename}_{count}'] = tempdf
                count+=1
            else:
                # print(f'{tablename} not in {i}')
                pass
        return pd.concat([i for i in tablepack.values()])
            # return tempdf
    except Exception as e:
        print(e)

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
    con = db.str
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

    with tqdm(total=len(df)) as pbar:
        """
        loop to use the progress bar on each iteration
        """
        for i, cdf in enumerate(chunker(df,chunksize)):
            replace = "replace" if i == 0 else "append"
            # area for on the fly pg/access type creation
            #  using alchemy_ret

            if access!=False:
                cdf.to_sql(name=f'{tablename}',
                           con=ret_access(acc_path),
                           index=False,
                           # dtype=onthefly,
                           if_exists=replace)
            # else clause will send to pg


            pbar.update(chunksize)
        tqdm._instances.clear()
    tqdm.write(f'{tablename} sent to db')

#
# def dbkey_gen(df,newfield, *fields):
#     df[f'{newfield}'] = (df[[f'{field.strip()}' for field in fields]].astype(str)).agg(''.join,axis=1).astype(object)
def dbkey_gen(df,newfield, *fields):
    dfcopy = df.copy()
    dfcopy[f'{newfield}'] = (df[[f'{field.strip()}' for field in fields]].astype(str)).agg(''.join,axis=1).astype(object)
    return dfcopy
