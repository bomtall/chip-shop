from pathlib import Path

import streamlit as st

import fryer.google_distance_matrix_api as gdm
import fryer.gov_dvla_vehicle_api as dvla
import fryer.logger
from fryer import config

st.set_page_config(
    page_title="chip-shop",
    page_icon="🍟",
    layout="wide",
    initial_sidebar_state="expanded",
)

key = Path(__file__).stem
logger = fryer.logger.get(key=key, path_log=config.get("FRYER_ENV_PATH_LOG"))


origin = st.text_input("Enter your origin postcode")
destination = st.text_input("Enter your destination postcode")
registration = st.text_input("Enter your vehicle registration")

if st.button("Calculate"):
    url = gdm.generate_google_distance_matrix_url(
        key=config.get("GOOGLE_DISTANCE_MATRIX_API_KEY"),
        origin_postcode=origin,
        destination_postcode=destination,
    )
    result = gdm.call_google_distance_matrix_api(url)
    distance_in_km = gdm.extract_distance_from_response(result)

    vehicle_info = dvla.get_dvla_vehicle_info(
        key=config.get("DVLA_API_KEY"), registration=registration
    )

    gco2_per_km = dvla.extract_gco2_per_km(vehicle_info)

    st.write(f"Distance: {distance_in_km} km")
    st.write(f"journey time: {result['rows'][0]['elements'][0]['duration']['text']}")
    st.write(
        f"Vehicle: {vehicle_info['make']} {vehicle_info['colour']} {vehicle_info['fuelType']}"
    )
    st.write(f"Co2 emissions: {distance_in_km * (gco2_per_km / 1000)} kgco2")
