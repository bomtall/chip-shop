import folium
import geopandas as gpd
import polars as pl
import streamlit as st
from streamlit_folium import st_folium

from fryer import all as fryer

collision = None
gdf = None


@st.cache_data
def get_data() -> gpd.GeoDataFrame:
    collision = fryer.data.uk_gov_dept_for_transport_road_accident.read_collision()
    gdf = fryer.data.ons_local_authority_district_boundaries.read()
    collisions_count = (
        collision.filter(pl.col("local_authority_ons_district").is_not_null())
        .group_by("local_authority_ons_district")
        .agg(
            [
                pl.col("accident_index").count().alias("accidents"),
                pl.col("number_of_casualties").sum().alias("casualties"),
                pl.col("number_of_vehicles").sum().alias("vehicles"),
            ],
        )
    )

    gdf2 = gdf.merge(
        collisions_count.to_pandas(),
        left_on="LAD24CD",
        right_on="local_authority_ons_district",
        how="left",
    )

    gdf2["geometry"] = gpd.GeoSeries(gdf2["geometry"]).simplify(tolerance=0.001)
    return gdf2


def create_map(data: gpd.GeoDataFrame) -> folium.Map:
    if "map" not in st.session_state or st.session_state.map is None:
        uk_map = fryer.map.create()

        colours = ["Reds", "Blues", "Greens"]

        for index, col in enumerate(
            ["accidents"]
        ):  # ["accidents", "casualties", "vehicles"]
            cp = folium.Choropleth(
                geo_data=data,
                name=col,
                data=data,
                columns=["LAD24CD", col],
                key_on="feature.properties.LAD24CD",
                fill_color=colours[index],
                bins=6,
                fill_opacity=0.6,
                line_opacity=0.4,
                smooth_factor=0,
                overlay=True,
                highlight=True,
            )
            cp.add_to(uk_map)

            folium.GeoJsonTooltip(["LAD24NM", "LAD24CD", col], localize=True).add_to(
                cp.geojson
            )
        folium.LayerControl().add_to(uk_map)
        st.session_state.map = uk_map  # Save the map in the session state

    # alternative is to return st.session_state.map
    return uk_map


if __name__ == "__main__":
    output = st_folium(create_map(get_data()))
    st.write(output)
