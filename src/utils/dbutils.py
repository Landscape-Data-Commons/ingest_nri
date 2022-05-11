from configparser import ConfigParser
import psycopg2.pool as pgpool

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
    def __init__(self, keyword = None):
        if keyword==None:
            self.params = config()
            self.str_1 = pgpool.SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
            self.str = self.str_1.getconn()
        else:
            self.params = config(section=f"{keyword}")
            self.params["options"] = "-c search_path=public"
            self.str_1 = pgpool.SimpleConnectionPool(minconn=1,maxconn=10,**self.params)
            self.str = self.str_1.getconn()
