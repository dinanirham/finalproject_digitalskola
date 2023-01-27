from sqlalchemy import create_engine

class Connector():
    def __init__(self):
        pass

    def conn_mysql(self, user, password, host, port, db):
        engine = create_engine("mysql+mysqlconnector://{}:{}@{}:{}/{}".
                 format(user, password, host, port, db)
        )
        return engine

    def conn_postgresql(self, user, password, host, port, db):
        engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".
                 format(user, password, host, port, db)
        )
        return engine 