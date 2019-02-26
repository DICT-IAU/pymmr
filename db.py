from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session, sessionmaker, scoped_session
from sqlalchemy import create_engine
from sqlalchemy import inspect

Base = automap_base()

database_username = 'root'
database_password = 'root'
database_ip       = 'localhost'
database_name     = 'mmr'
engine = create_engine('mysql://{0}:{1}@{2}/{3}'.
                     format(database_username, database_password, 
                     database_ip, database_name), pool_pre_ping=True, pool_size=50, max_overflow=100)

db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=engine))
# reflect the tables
Base.prepare(engine, reflect=True)

# mapped classes are now created with names by default
Virus = Base.classes.virus

Base.query = db_session.query_property()

def row2dict(obj):
    return {c.key: getattr(obj, c.key)
            for c in inspect(obj).mapper.column_attrs}

def result2dict(result):
    return [row2dict(i) for i in result]
