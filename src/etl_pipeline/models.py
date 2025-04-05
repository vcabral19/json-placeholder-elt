from typing import Optional
from sqlmodel import SQLModel, Field, Relationship

class Geo(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    lat: str
    lng: str
    address_id: Optional[int] = Field(default=None, foreign_key="address.id")

class Address(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    street: str
    suite: str
    city: str
    zipcode: str
    # One-to-one relationship with Geo. Note: uselist=False ensures a one-to-one relation.
    geo: Optional[Geo] = Relationship(back_populates="address", sa_relationship_kwargs={"uselist": False})

Geo.address = Relationship(back_populates="geo")

class Company(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    catchPhrase: str
    bs: str

class User(SQLModel, table=True):
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    username: str
    email: str
    phone: str
    website: str
    # Foreign keys for the nested models
    address_id: Optional[int] = Field(default=None, foreign_key="address.id")
    company_id: Optional[int] = Field(default=None, foreign_key="company.id")
    # Optional field to store the entire raw record
    raw: Optional[dict] = Field(default=None)

    # Relationships to nested models
    address: Optional[Address] = Relationship()
    company: Optional[Company] = Relationship()
