from Car import Car
from GenericRequestor import GenericRequestor
from Logger import Logger

class TempestRequestor(GenericRequestor):
    def __init__(self, logger):
        super().__init__(logger)

    def parse_response(self):
        """Tempest-specific parser for received JSON. Returns a hash set."""
        
        results = self.response["results"]
        self.logger.write_to_file(f"Converting {len(results)} results to Car objects")
        return_set = set()

        for listing in results:
            try:
                return_set.add(
                    Car(
                        make = listing["make"],
                        model = listing["model"],
                        year = listing["year"],
                        vin = listing["vin"],
                        location_zipcode = listing["locationCode"],
                        location = listing["location"],
                        mileage = listing["mileage"],
                        price = listing["price"],
                        url = listing["url"],
                        listing_id = listing["id"],
                        listing_date = listing["date"],
                        details = listing["details"],
                        fetch_ts = self.get_timestamp()
                    )
                )
            except Exception as e:
                self.logger.write_to_file("Encountered error while converting to Car objects.")
                self.logger.write_to_file(f"Error: {e}")
                self.logger.write_to_file(f"Occurred at:\n{listing}")

        return return_set