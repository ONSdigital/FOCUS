"""module used to test sqlaclamy..."""

import os
import sys
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy import create_engine

Base = declarative_base()


# contains the information about the types of household
class Household_type(Base):
    __tablename__ = 'household_type'
    # Here we define columns for the table address.
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)  # unique id for the type of hh (e.g. htc1,2, old, student etc)
    type = Column(String(250), nullable=False)   # string showing the type
    paper_propensity = Column(Integer, nullable=False)  # an attribute of that type of hh


# the below creates the classes that represents the tables in the database - so the schema to follow
class Household(Base):
    __tablename__ = 'household'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)  # unique identifier for the household
    type_id = Column(Integer, ForeignKey('household_type.id'))  # link to hh_type tabe
    household_type = relationship(Household_type)


# Create an engine that stores data in the local directory's sqlalchemy_example.db file.
engine = create_engine('sqlite:///sqlalchemy_example.db')

# Create all tables in the engine. This is equivalent to "Create Table" statements in raw SQL.
Base.metadata.create_all(engine)