from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)    #Destination is in sets, so each Car() object needs to be immutable
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
    listing_date: Optional[str]
    details: Optional[str]
    fetch_ts: str