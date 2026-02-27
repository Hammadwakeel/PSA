import logging
import requests
import urllib3
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class RgenBackendError(Exception):
    """Raised when RGEN API request or login fails."""
    pass


class RgenClient:
    def __init__(
        self,
        base_url: str = "https://api.rgen.com",
        email: Optional[str] = None,
        password: Optional[str] = None,
        token: Optional[str] = None,
        verify: bool = True,
        timeout: int = 90,
    ):
        """
        Lightweight RGEN API client.

        Provide either:
            - email + password  (for login)
            - OR token directly

        Set verify=False only if using self-signed SSL in development.
        """

        self.base_url = base_url.rstrip("/")
        self.email = email
        self.password = password
        self._token = token
        self.timeout = timeout

        self.session = requests.Session()
        self.session.verify = verify

        if not verify:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # ---------------------------------------------------------
    # Context Manager Support (Fixes __enter__ error)
    # ---------------------------------------------------------

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()

    # ---------------------------------------------------------
    # Internal Helpers
    # ---------------------------------------------------------

    def _auth_header(self) -> Dict[str, str]:
        if not self._token:
            if self.email and self.password:
                self.login()
            else:
                raise RuntimeError("No authentication method provided.")
        return {"Authorization": f"Bearer {self._token}"}

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        logger.debug("_request | method=%s path=%s", method, path)

        try:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
            headers.update(self._auth_header())

            response = self.session.request(
                method=method,
                url=url,
                params=params,
                json=json,
                headers=headers,
                timeout=self.timeout,
            )
        except requests.exceptions.Timeout as e:
            logger.error("_request timeout | method=%s path=%s timeout=%s", method, path, self.timeout)
            raise RgenBackendError(f"Request timeout: {path}") from e
        except requests.exceptions.RequestException as e:
            logger.exception("_request failed | method=%s path=%s error=%s", method, path, str(e))
            raise RgenBackendError(f"Request failed: {path}: {e}") from e

        logger.info("_request response | method=%s path=%s status=%s", method, path, response.status_code)

        if response.status_code >= 500:
            logger.error(
                "_request server error | path=%s status=%s body_preview=%s",
                path,
                response.status_code,
                (response.text or "")[:200],
            )
            raise RgenBackendError(f"Server Error {response.status_code}: {response.text}")

        if response.status_code == 401:
            logger.warning("_request 401, re-login and retry | path=%s", path)
            try:
                self.login()
                headers.update(self._auth_header())
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers=headers,
                    timeout=self.timeout,
                )
            except Exception as retry_err:
                logger.exception("_request retry failed after re-login | path=%s", path)
                raise RgenBackendError(f"Retry after 401 failed: {retry_err}") from retry_err
            logger.info("_request retry response | path=%s status=%s", path, response.status_code)

        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            logger.error("_request HTTP error | path=%s status=%s", path, response.status_code)
            raise RgenBackendError(f"HTTP {response.status_code}: {response.text}") from e

        try:
            return response.json()
        except ValueError:
            return response.text

    # ---------------------------------------------------------
    # Public API Methods
    # ---------------------------------------------------------

    def login(self) -> str:
        """Login and store access token."""
        if not (self.email and self.password):
            raise ValueError("Email and password required for login")

        url = f"{self.base_url}/api/v1/auth/login"
        payload = {
            "email": self.email,
            "password": self.password,
            "workspace_id": None,
            "remember_me": False,
        }

        try:
            response = self.session.post(url, json=payload, timeout=self.timeout)
            response.raise_for_status()
        except requests.exceptions.Timeout as e:
            logger.error("login timeout | url=%s", url)
            raise RgenBackendError("Login timeout") from e
        except requests.exceptions.RequestException as e:
            logger.exception("login request failed | error=%s", str(e))
            raise RgenBackendError(f"Login request failed: {e}") from e

        try:
            data = response.json()
        except ValueError as e:
            logger.error("login invalid JSON response")
            raise RgenBackendError("Login: invalid JSON response") from e

        token = None
        try:
            token = data.get("data", {}).get("access_token")
        except (AttributeError, TypeError):
            pass

        if not token:
            logger.error("login failed: access token missing in response")
            raise RgenBackendError("Login failed. Access token missing.")

        self._token = token
        logger.info("login succeeded | base_url=%s", self.base_url)
        return token

    def list_data_sources(self) -> Any:
        try:
            return self._request("GET", "/api/v1/data-sources")
        except RgenBackendError:
            raise
        except Exception as e:
            logger.exception("list_data_sources failed")
            raise RgenBackendError(f"list_data_sources failed: {e}") from e

    def get_data_source(self, data_source_id: int) -> Any:
        try:
            return self._request("GET", f"/api/v1/data-sources/{data_source_id}")
        except RgenBackendError:
            raise
        except Exception as e:
            logger.exception("get_data_source failed | data_source_id=%s", data_source_id)
            raise RgenBackendError(f"get_data_source failed: {e}") from e

    def test_connection(self, data_source_id: int) -> Any:
        try:
            return self._request("POST", f"/api/v1/data-sources/{data_source_id}/test")
        except RgenBackendError:
            raise
        except Exception as e:
            logger.exception("test_connection failed | data_source_id=%s", data_source_id)
            raise RgenBackendError(f"test_connection failed: {e}") from e

    def discover_schema(self, data_source_id: int, refresh: bool = True) -> Any:
        params = {"refresh": refresh}
        try:
            return self._request(
                "POST",
                f"/api/v1/data-sources/{data_source_id}/discover-schema",
                params=params,
            )
        except RgenBackendError:
            raise
        except Exception as e:
            logger.exception("discover_schema failed | data_source_id=%s", data_source_id)
            raise RgenBackendError(f"discover_schema failed: {e}") from e

    def get_schemas(self, data_source_id: int, include_columns: bool = True) -> Any:
        params = {"include_columns": include_columns}
        try:
            return self._request(
                "GET",
                f"/api/v1/data-sources/{data_source_id}/schemas",
                params=params,
            )
        except RgenBackendError:
            raise
        except Exception as e:
            logger.exception("get_schemas failed | data_source_id=%s", data_source_id)
            raise RgenBackendError(f"get_schemas failed: {e}") from e
    
    def get_tools(self):
        """Return tool declarations for Gemini function calling (name, description, parameters)."""
        return [
            {
                "name": "list_data_sources",
                "description": "Fetch all data sources available in the RGEN workspace.",
                "parameters": {"type": "object", "properties": {}}
            },
            {
                "name": "get_data_source",
                "description": "Get details for a specific data source by ID.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "data_source_id": {"type": "integer", "description": "The ID of the data source."}
                    },
                    "required": ["data_source_id"]
                }
            },
            {
                "name": "test_connection",
                "description": "Test connection to a data source by ID. Call this first before discover_schema.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "data_source_id": {"type": "integer", "description": "The ID of the data source to test."}
                    },
                    "required": ["data_source_id"]
                }
            },
            {
                "name": "discover_schema",
                "description": "Run schema discovery on a specific data source. Call after test_connection succeeds.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "data_source_id": {"type": "integer", "description": "The ID of the data source."},
                        "refresh": {"type": "boolean", "description": "Whether to force a refresh."}
                    },
                    "required": ["data_source_id"]
                }
            },
            {
                "name": "get_schemas",
                "description": "Get schemas (and optional columns) for a data source. Used for data preview.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "data_source_id": {"type": "integer", "description": "The ID of the data source."},
                        "include_columns": {"type": "boolean", "description": "Whether to include column details."}
                    },
                    "required": ["data_source_id"]
                }
            }
        ]

    def execute_tool_real(self, tool_name: str, **kwargs) -> Any:
        """Execute a tool by name with the given kwargs. Used by the planning agent."""
        logger.debug("execute_tool_real | tool=%s kwargs_keys=%s", tool_name, list(kwargs.keys()))
        try:
            if tool_name == "list_data_sources":
                return self.list_data_sources()
            if tool_name == "get_data_source":
                return self.get_data_source(kwargs["data_source_id"])
            if tool_name == "discover_schema":
                return self.discover_schema(
                    kwargs["data_source_id"],
                    refresh=kwargs.get("refresh", True),
                )
            if tool_name == "test_connection":
                return self.test_connection(kwargs["data_source_id"])
            if tool_name == "get_schemas":
                return self.get_schemas(
                    kwargs["data_source_id"],
                    include_columns=kwargs.get("include_columns", True),
                )
            logger.error("execute_tool_real unknown tool | tool=%s", tool_name)
            raise ValueError(f"Unknown tool: {tool_name}")
        except (ValueError, KeyError) as e:
            logger.warning("execute_tool_real bad args | tool=%s error=%s", tool_name, str(e))
            raise
        except RgenBackendError:
            raise
        except Exception as e:
            logger.exception("execute_tool_real failed | tool=%s error=%s", tool_name, str(e))
            raise RgenBackendError(f"Tool {tool_name} failed: {e}") from e


# =============================================================
# MAIN EXECUTION BLOCK
# =============================================================

# if __name__ == "__main__":

#     with RgenClient(
#         email="daavel1@gmail.com",
#         password="SecurePass123!",
#         verify=False,  # Set True in production
#     ) as client:

#         print("\n✅ Logging in...")
#         token = client.login()
#         print("Access Token:", token)

#         print("\n📋 Listing Data Sources...")
#         ds_list = client.list_data_sources()
#         if ds_list['success']:
#             status = "sucessfully executed"
#         else:
#             status = "failed"
#         # print(ds_list)

#         items = ds_list.get("data", {}).get("items", [])

#         if not items:
#             raise RuntimeError("No data sources found.")

#         data_source_id = items[0]["id"]
#         print("\n🎯 Using Data Source ID:", data_source_id)

#         print("\n🔍 Getting Data Source Details...")
#         print(client.get_data_source(data_source_id))

#         print("\n🧪 Testing Connection...")
#         print(client.test_connection(data_source_id))

#         print("\n🔄 Discovering Schema...")
#         print(client.discover_schema(data_source_id, refresh=True))

#         # print("\n📑 Getting Schemas...")
#         # print(client.get_schemas(data_source_id, include_columns=False))
 
#         print("\n✅ All endpoints executed successfully.")