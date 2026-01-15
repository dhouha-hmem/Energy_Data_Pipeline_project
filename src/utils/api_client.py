import requests
from typing import Dict, Any


class EnergyChartsClient:
    """
    Simple API client for https://api.energy-charts.info
    Responsibility:
    - Build correct URLs
    - Send HTTP GET requests
    - Return parsed JSON responses
    """

    def __init__(self, base_url: str, timeout_seconds: int = 60):
        #Initialize the API client
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout_seconds  #max wait time for API response

    def get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generic GET request to Energy-Charts API:
        - Builds full URL
        - Sends request with parameters
        - Raises error if request fails
        - Returns JSON response as Python dict
        """
        url = f"{self.base_url}{endpoint}"

        response = requests.get(url, params=params, timeout=self.timeout)
        # Raise exception for HTTP errors
        response.raise_for_status()

        return response.json()

    def get_public_power(
        self,
        country: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Fetch public power production data for a country.
        Returns:
        - Raw JSON response from Energy-Charts API
        """
        return self.get(
            endpoint="/public_power",
            params={
                "country": country,
                "start": start_date,
                "end": end_date
            }
        )

    def get_price(
        self,
        market: str,
        start_date: str,
        end_date: str
    ) -> Dict[str, Any]:
        """
        Fetch electricity price data for a bidding zone (e.g. DE-LU).
        Returns:
        - Raw JSON response with prices and timestamps
        """
        return self.get(
            endpoint="/price",
            params={
                "bzn": market.upper(),      # API expects uppercase
                "start": start_date,
                "end": end_date
            }
        )
