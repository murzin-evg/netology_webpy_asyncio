from sqlalchemy import String, Column, Integer
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker


DSN = 'postgresql+asyncpg://postgres:postgres@localhost:5432/swapi_db'

engine = create_async_engine(DSN)

Session = sessionmaker(
    class_=AsyncSession,
    expire_on_commit=False,
    bind=engine
)

Base = declarative_base()


class SWAPI(Base):
    __tablename__ = "swapi"

    id = Column(Integer, primary_key=True)
    birth_year = Column(String)
    eye_color = Column(String)
    films = Column(String)
    gender = Column(String)
    hair_color = Column(String)
    height = Column(String)
    homeworld = Column(String)
    mass = Column(String)
    name = Column(String)
    skin_color = Column(String)
    species = Column(String)
    starships = Column(String)
    vehicles = Column(String)
