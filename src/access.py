import os, sqlalchemy
import os.path
import pandas as pd
import numpy as np
from utils import db, sql_str, config, Acc
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
#

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

def header_build(path,tablename):
    basepath = os.path.dirname(path) # ingestables/nriupdate
    basebase = os.path.dirname(basepath) # ingestables/



    cols = type_lookup(basebase, tablename.upper(), "types")
    tempdf = pd.read_csv(os.path.join(path,f'{tablename}.txt'), sep='|', index_col=False, names=cols.keys())


    fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
    for field in tempdf.columns:
        if (cols[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
            tempdf[field] = tempdf[field].apply(lambda i: i.strip())
            tempdf[field] = pd.to_numeric(tempdf[field])

    if 'COUNTY' in tempdf.columns:
        tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

    if 'STATE' in tempdf.columns:
        tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')

    dot_list = ['HIT1','HIT2','HIT3', 'HIT4', 'HIT5', 'HIT6', 'NONSOIL']
    if field in dot_list:
        tempdf[field] = tempdf[field].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)

                            ##### STRIP ANYWAY
    if tempdf[field].dtype==np.object:
        tempdf[field] = tempdf[field].apply(lambda i: i.strip() if type(i)!=float else i)


    less_fields = ['statenm','countynm']
    if tablename not in less_fields:
        tempdf = dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
        tempdf = dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
    tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])

    return tempdf

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


def dbkey_gen(df,newfield, *fields):
    df[f'{newfield}'] = (df[[f'{field.strip()}' for field in fields]].astype(str)).agg(''.join,axis=1).astype(object)
