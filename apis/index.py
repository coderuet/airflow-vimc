import requests
from requests.auth import HTTPBasicAuth
from enum import Enum


class AuthType(Enum):
    BASIC = 1
    BEARER = 2
    API_KEY = 3
    OAUTH = 4


class APIClient:
    """Simple API helper that centralizes auth, headers, and response parsing."""

    def __init__(
        self,
        base_url,
        auth_type=AuthType.BASIC,
        username=None,
        password=None,
        token=None,
        api_key=None,
        session=None,
    ):
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type
        self.username = username
        self.password = password
        self.token = token
        self.api_key = api_key
        self.session = session or requests.Session()

    def _handle_response(self, response):
        try:
            response.raise_for_status()
            json_data = response.json()
            return json_data

        except requests.exceptions.HTTPError as err:
            return {"success": False, "error": str(err), "code": response.status_code}
        except Exception as err:
            return {"success": False, "error": str(err)}

    def _build_headers(self, headers):
        merged = {"Accept": "application/json"}
        if headers:
            merged.update(headers)

        if self.auth_type == AuthType.BEARER and self.token:
            merged.setdefault("Authorization", f"Bearer {self.token}")
        elif self.auth_type == AuthType.API_KEY and self.api_key:
            merged.setdefault("X-API-Key", self.api_key)

        return merged

    def _build_auth(self):
        if self.auth_type == AuthType.BASIC and self.username and self.password:
            return HTTPBasicAuth(self.username, self.password)
        return None

    def _request(self, method, endpoint, **kwargs):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        headers = self._build_headers(kwargs.pop("headers", None))
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            auth=self._build_auth(),
            timeout=kwargs.pop("timeout", 30),
            **kwargs,
        )
        return self._handle_response(response)

    def get(self, endpoint, params=None, headers=None):
        return self._request("GET", endpoint, params=params, headers=headers)

    def post(self, endpoint, data=None, json=None, headers=None):
        return self._request("POST", endpoint, data=data, json=json, headers=headers)

    def close(self):
        self.session.close()
