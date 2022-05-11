import os
import access as acc

def batcher(nriupdate_path, acc_path, mdb=True):
    """
    prepare each table accross the the ranges found in the
    ingestables/nrifolder directory.

    args:
        nri_update_path: directory with  nri tables in .txt format
        acc_path:  path to access file
        mdb: boolean =
    """
    tables = {}
    tablelist = table_list_creator(nriupdate_path)

    # collect all tables
    for i in tablelist:
        df = acc.header_build(nriupdate_path, i)
        tables[i] = df.drop_duplicates()
    # send all tables
    for k,v in tables.items():
        acc.pg_send(v,acc_path,k, access=True if mdb==True else False)


def table_list_creator(filedirectory):
    table_master_set = set()
    nriupdate_dirs = [i for i in os.listdir(filedirectory) if (not i.endswith(".accdb") and not i.endswith(".laccdb")) and ("Coordinates" not in i)]
    for i in nriupdate_dirs:
        # print(i)
        for j in [k.split('.')[0] for k in os.listdir(os.path.join(filedirectory,i))]:
            table_master_set.add(j)
    return [i for i in table_master_set]
