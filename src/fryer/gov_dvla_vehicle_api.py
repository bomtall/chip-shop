import requests

__all__ = ["get_dvla_vehicle_info"]


def get_dvla_vehicle_info(registration: str, key: str) -> dict:
    """Get vehicle information from the DVLA API."""
    url = "https://driver-vehicle-licensing.api.gov.uk/vehicle-enquiry/v1/vehicles"
    payload = f"""{{
    "registrationNumber": "{registration}"
    }}"""
    headers = {"x-api-key": key, "Content-Type": "application/json"}
    response = requests.post(url, headers=headers, data=payload, timeout=10)

    return response.json()
