from sqlalchemy import create_engine, MetaData, text
from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.orm import sessionmaker, declarative_base

# Параметры подключения к базе данных
db_url = "sqlite:///cheaters.db"
engine = create_engine(db_url)
meta = MetaData()
Base = declarative_base()


# Определение таблицы Cheater
class Cheater(Base):
    __tablename__ = 'cheaters'

    player_id = Column(Integer, primary_key=True)
    ban_time = Column(String)


# Определение таблицы Error
class Error(Base):
    __tablename__ = 'errors'

    timestamp = Column(Integer)
    player_id = Column(Integer)
    event_id = Column(Integer)
    error_id = Column(String, primary_key=True)
    json_server = Column(String)
    json_client = Column(String)


# Создание сессии
Session = sessionmaker(bind=engine)
session = Session()


# Создание таблиц
def create_tables():
    return meta.create_all(engine)


# Получение сессии
def get_session():
    return session


# Создание таблицы errors, если она не существует
def create_table_errors():
    with get_session() as session:
        session.execute(text('CREATE TABLE IF NOT EXISTS errors ('
                             'timestamp INTEGER, '
                             'player_id INTEGER, '
                             'event_id INTEGER, '
                             'error_id TEXT PRIMARY KEY, '
                             'json_server TEXT, '
                             'json_client TEXT)'))
    return
