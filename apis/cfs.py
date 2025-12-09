from .index import APIClient
from typing import Dict


class CfsAPI(APIClient):
    """
    Typed wrapper around the Category endpoints. Every method returns the raw JSON payload.
    """
    ENDPOINT_MAP = {
        "get_trialbalance": "/get_trialbalance",
        "get_purchase": "/get_purchase",
        "get_sale": "/get_sale",
        "get_ar": "/get_ar",
        "get_ap": "/get_ap",
    }

    def fetch_resource(self, resource_name: str, json: Dict):
        """
        Generic helper that fetches a resource by logical name.
        """
        print(f"----Running fetch data  {resource_name}------")
        endpoint = self.ENDPOINT_MAP.get(resource_name)
        if not endpoint:
            raise ValueError(
                f"Unsupported Category resource '{resource_name}'")
        json_data = self.post(endpoint, json=json)
        data_res = json_data['Data']
        error_code = json_data["ErrorCode"]
        if error_code == "1":
            return data_res
        raise RuntimeError(
            f"CFS API call failed for '{resource_name}': {json_data.get('ErrorMsg')} "
            f"(code={json_data.get('ErrorCode')})"
        )

    def fetch_many(self, resource_names, json: Dict):
        """
        Fetch multiple resources and return a mapping resource_name -> payload.
        """
        results = {}
        for resource in resource_names:
            results[resource] = self.fetch_resource(resource, json)
        return results
