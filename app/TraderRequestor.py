from Car import Car
from GenericRequestor import GenericRequestor
from Logger import Logger

class TraderRequestor(GenericRequestor):
    def __init__(self):
        super().__init__()

    def parse_response(self):
        results = self.response["listings"]
        self.logger.write_to_file(f"Converting {len(results)} results to Car objects")
        return_set = set()

        for listing in results:
            try:
                return_set.add(
                    Car(
                        make = listing["make"]["name"],
                        model = listing["model"]["name"],
                        year = str(listing["year"]),
                        vin = listing["vin"],
                        location_zipcode = listing["owner"]["location"]["address"]["zip"],
                        location = listing["owner"]["location"]["address"]["city"] + ', ' + listing["owner"]["location"]["address"]["state"],
                        mileage = listing["mileage"]["value"],
                        price = listing["pricingDetail"]["salePrice"],
                        url = listing["owner"]["website"]["href"],
                        listing_id = listing["id"],
                        details = listing["details"],
                        fetch_ts = self.get_timestamp()
                    )
                )
            except Exception as e:
                self.logger.write_to_file("Encountered error while converting to Car objects.")
                self.logger.write_to_file(f"Error: {e}")
                self.logger.write_to_file(f"Occurred at:\n{listing}")

        return return_set