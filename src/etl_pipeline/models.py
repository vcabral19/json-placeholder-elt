import hashlib
from typing import Optional
from pydantic import BaseModel
from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, PrimaryKeyConstraint
from sqlalchemy.types import JSON

# ---------------------------
# Ingestion Models
# ---------------------------

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
    # One-to-one relationship with Geo.
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
    __table_args__ = (
        PrimaryKeyConstraint("user_id", "extraction_ts", name="user_pk"),
        {"extend_existing": True},
    )
    # Original API id
    user_id: int = Field()
    # Extraction timestamp (as Unix epoch seconds)
    extraction_ts: int = Field()
    name: str
    username: str
    email: str
    phone: str
    website: str
    address_id: Optional[int] = Field(default=None, foreign_key="address.id")
    company_id: Optional[int] = Field(default=None, foreign_key="company.id")
    raw: Optional[dict] = Field(default=None, sa_column=Column(JSON))
    
    address: Optional["Address"] = Relationship(
        sa_relationship_kwargs={"cascade": "all, delete-orphan", "single_parent": True}
    )
    company: Optional["Company"] = Relationship(
        sa_relationship_kwargs={"cascade": "all, delete-orphan", "single_parent": True}
    )
    
    class Config:
        extra = "allow"

    @classmethod
    def from_api(cls, record: dict, extraction_ts: int) -> "User":
        # Process nested address and company as before.
        address_data = record.get("address", {}).copy()
        geo_data = address_data.pop("geo", None)
        geo = Geo(**geo_data) if geo_data else None
        address = Address(**address_data, geo=geo) if address_data else None
        company_data = record.get("company", {})
        company = Company(**company_data) if company_data else None
        return cls(
            user_id=record["id"],
            extraction_ts=extraction_ts,
            name=record["name"],
            username=record["username"],
            email=record["email"],
            phone=record["phone"],
            website=record["website"],
            address=address,
            company=company,
            raw=record
        )

    def transform(self, extraction_iso: str) -> dict:
        """
        Transforms this User instance into processed entities.
        Returns a dict mapping:
          - "processed_company" to a ProcessedCompany instance (if company exists)
          - "processed_user" to a ProcessedUser instance
        """
        result = {}
        if self.company:
            # Generate a stable company_id using an MD5 hash of the company name.
            h = hashlib.md5(self.company.name.encode()).hexdigest()[:8]
            company_id = int(h, 16)
            processed_company = ProcessedCompany.from_company(self.company, extraction_iso)
            result["processed_company"] = processed_company
            processed_user = ProcessedUser.from_user(self, extraction_iso, company_id)
            result["processed_user"] = processed_user
        else:
            processed_user = ProcessedUser.from_user(self, extraction_iso, None)
            result["processed_user"] = processed_user
        return result

# ---------------------------
# Processed Models (for Transformation)
# ---------------------------

class ProcessedCompany(BaseModel):
    company_id: int
    name: str
    catchPhrase: str
    bs: str
    extraction_ts: str
    # Used to determine the output folder name.
    path_name: str = "processed_company"

    @classmethod
    def from_company(cls, company: Company, extraction_ts: str) -> "ProcessedCompany":
        h = hashlib.md5(company.name.encode()).hexdigest()[:8]
        company_id = int(h, 16)
        return cls(
            company_id=company_id,
            name=company.name,
            catchPhrase=company.catchPhrase,
            bs=company.bs,
            extraction_ts=extraction_ts
        )

    class Config:
        orm_mode = True

class ProcessedUser(BaseModel):
    user_id: int
    username: str
    phone: str
    email: str
    website: str
    company_id: Optional[int]
    extraction_ts: str
    path_name: str = "processed_user"

    @classmethod
    def from_user(cls, user: User, extraction_iso: str, company_id: Optional[int]) -> "ProcessedUser":
        return cls(
            user_id=user.user_id,
            username=user.username,
            phone=user.phone,
            email=user.email,
            website=user.website,
            company_id=company_id,
            extraction_ts=extraction_iso
        )

    class Config:
        orm_mode = True
