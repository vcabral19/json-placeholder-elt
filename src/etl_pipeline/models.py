from pydantic import BaseModel
from typing import Any

class Geo(BaseModel):
    lat: str
    lng: str

class Address(BaseModel):
    street: str
    suite: str
    city: str
    zipcode: str
    geo: Geo

class Company(BaseModel):
    name: str
    catchPhrase: str
    bs: str

class User(BaseModel):
    id: int
    name: str
    username: str
    email: str
    address: Address
    phone: str
    website: str
    company: Company

    # Allow extra fields so that non-compliant data isn't dropped
    # This is useful for not losing data when the API response changes
    # This non model compliant data can later be reprocessed
    class Config:
        extra = "allow"
