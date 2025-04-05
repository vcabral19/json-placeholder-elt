from typing import Optional
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column
from sqlalchemy.types import JSON

class Geo(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    lat: str
    lng: str
    address_id: Optional[int] = Field(default=None, foreign_key="address.id")
    # Reverse relationship (one-to-one) with Address.
    address: Optional["Address"] = Relationship(back_populates="geo")
    
    class Config:
        extra = "allow"

class Address(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    street: str
    suite: str
    city: str
    zipcode: str
    # One-to-one relationship with Geo. Cascade is set on the "one" side.
    geo: Optional[Geo] = Relationship(
        back_populates="address",
        sa_relationship_kwargs={"uselist": False, "cascade": "all, delete-orphan", "single_parent": True}
    )
    
    class Config:
        extra = "allow"

class Company(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    catchPhrase: str
    bs: str
    
    class Config:
        extra = "allow"

class User(SQLModel, table=True):
    __table_args__ = {"extend_existing": True}
    id: Optional[int] = Field(default=None, primary_key=True)
    name: str
    username: str
    email: str
    phone: str
    website: str
    address_id: Optional[int] = Field(default=None, foreign_key="address.id")
    company_id: Optional[int] = Field(default=None, foreign_key="company.id")
    # Explicitly set the column type so dict values are stored properly.
    raw: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    
    # The relationships below are many-to-one. To use delete-orphan cascade, we need single_parent=True.
    address: Optional[Address] = Relationship(
        sa_relationship_kwargs={"cascade": "all, delete-orphan", "single_parent": True}
    )
    company: Optional[Company] = Relationship(
        sa_relationship_kwargs={"cascade": "all, delete-orphan", "single_parent": True}
    )
    
    class Config:
        extra = "allow"

    @classmethod
    def from_api(cls, record: dict) -> "User":
        """
        Transforms an API record (with nested dicts) into a User instance with proper nested models.
        """
        # Copy address data and extract the geo part.
        address_data = record.get("address", {}).copy()
        geo_data = address_data.pop("geo", None)
        geo = Geo(**geo_data) if geo_data else None
        address = Address(**address_data, geo=geo) if address_data else None

        company_data = record.get("company", {})
        company = Company(**company_data) if company_data else None

        return cls(
            id=record["id"],
            name=record["name"],
            username=record["username"],
            email=record["email"],
            phone=record["phone"],
            website=record["website"],
            address=address,
            company=company,
            raw=record
        )
