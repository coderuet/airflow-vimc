from .index import APIClient


class CategoryAPI(APIClient):
    """
    Typed wrapper around the Category endpoints. Every method returns the raw JSON payload.
    """

    ENDPOINT_MAP = {
        "companies": "/companies",
        "departments": "/departments",
        "positions": "/positions",
        "ship_details": "/ship-details",
        "goods": "/goods",
        "services": "/services",
        "handling_methods": "/handling-methods",
        "annual_plan_cb": "/annual-plan/cb",
        "annual_plan_vtb": "/annual-plan/vtb",
        "monthly_plan_cb": "/monthly-plan/cb",
        "monthly_plan_vtb": "/monthly-plan/vtb",
        "job_grades": "/job-grades",
    }

    def fetch_resource(self, resource_name: str, params=None):
        """
        Generic helper that fetches a resource by logical name.
        """
        endpoint = self.ENDPOINT_MAP.get(resource_name)
        if not endpoint:
            raise ValueError(
                f"Unsupported Category resource '{resource_name}'")

        response = self.get(endpoint, params=params)
        if response["success"]:
            return response["data"]
        raise RuntimeError(
            f"Category API call failed for '{resource_name}': {response.get('error')} "
            f"(code={response.get('code')})"
        )

    def fetch_many(self, resource_names, params=None):
        """
        Fetch multiple resources and return a mapping resource_name -> payload.
        """
        results = {}
        for resource in resource_names:
            results[resource] = self.fetch_resource(resource, params=params)
        return results
