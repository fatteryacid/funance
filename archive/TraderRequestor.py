from Car import Car
from GenericRequestor import GenericRequestor
from Logger import Logger

class TraderRequestor(GenericRequestor):
    """This child class will be deprecated since we are now only dumping raw JSON into the database landing site."""
    def __init__(self, logger):
        super().__init__(logger)

    def parse_response(self):
        """Trader-specific parser for received JSON. Returns a hash set."""

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
                        listing_date = None,    # can't find it
                        details = listing["details"],
                        fetch_ts = self.get_timestamp()
                    )
                )
            except Exception as e:
                self.logger.write_to_file("Encountered error while converting to Car objects.")
                self.logger.write_to_file(f"Error: {e}")
                self.logger.write_to_file(f"Occurred at:\n{listing}")

        return return_set