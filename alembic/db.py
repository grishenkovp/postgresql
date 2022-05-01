from sqlalchemy import (create_engine,
                        Column,
                        Integer,
                        String,
                        Text,
                        Date,
                        Numeric,
                        ForeignKey)
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship

from settings import settings

dialect = settings.db_dialect
driver = settings.db_driver
username = settings.db_username
password = settings.db_password
host = settings.server_host
port = settings.server_port
database = settings.db_name

database_url = f'{dialect}+{driver}://{username}:{password}@{host}:{port}/{database}'

print(database_url)

engine = create_engine(database_url)

Base = declarative_base()

class Manager(Base):
    __tablename__ = "managers"

    id = Column(Integer, primary_key=True)
    manager_name = Column(String(255), unique=True, nullable=False)
    description = Column(String, nullable=True)
    sale = relationship('Sale')

class Region(Base):
    __tablename__ = "regions"

    id = Column(Integer, primary_key=True)
    region_name = Column(String(255), unique=True, nullable=False)
    description = Column(String, nullable=True)
    sale = relationship('Sale')

class Product(Base):
    __tablename__ = "products"

    id = Column(Integer, primary_key=True)
    product_name = Column(String(255), unique=True, nullable=False)
    description = Column(String, nullable=True)
    sale = relationship('Sale')

class Sale(Base):
    __tablename__ = 'sales'

    id = Column(Integer, primary_key=True)
    date = Column(Date, nullable=False)
    region_id = Column(Integer, ForeignKey('regions.id'), nullable=False)
    manager_id = Column(Integer, ForeignKey('managers.id'), nullable=False)
    products_id = Column(Integer, ForeignKey('products.id'), nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    quantity = Column(Integer, nullable=False)
    amount = Column(Numeric(10, 2), nullable=False)
    description = Column(String, nullable=True)


# Base.metadata.create_all(engine)