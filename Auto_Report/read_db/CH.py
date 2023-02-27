import pandahouse
import os

class GetClickhouse:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf

    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

class InsertClickhouse:
    def __init__(self, data, table_name, db = 'simulator'):
        self.connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': os.environ.get("CONNECTION_PASSWORD"),
        'user': os.environ.get("CONNECTION_LOGIN"),
        'database': db,
        }
        self.data = data
        self.table_name = table_name
        self.to_clickhouse_df

    @property
    def to_clickhouse_df(self):
        try:
            self.insert = pandahouse.to_clickhouse(self.data, self.table_name, index=False, connection=self.connection)

        except Exception as err:
            print("\033[31m {}".format(err))
            #exit(0)
