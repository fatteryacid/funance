from dataclasses import dataclass
from typing import Optional

@dataclass
class Car:
    make: str
    model: str
    year: str
    vin: str
    location_zipcode: str
    location: str
    mileage: str
    price: str
    url: str
    listing_id: str
    details: Optional[str]
    fetch_ts: str