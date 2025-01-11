import pytest

import fryer.data


@pytest.mark.integration
def test_get_all_services_available_online():
    df = fryer.data.uk_gov_ons_geo.get_all_services_available_online()
    assert not df.is_empty()


@pytest.mark.integration
def test_download_features():
    url = "https://services1.arcgis.com/ESMARspQHYMw9BZ9/arcgis/rest/services/Countries_December_2023_Boundaries_UK_BFC/FeatureServer/0/query?outFields=*&where=1%3D1+AND+FID+BETWEEN+1+AND+4&f=geojson"
    features = fryer.data.uk_gov_ons_geo.download_features(url=url)
    assert len(features) == 4


@pytest.mark.integration
@pytest.mark.parametrize(
    "boundaries_type",
    [
        fryer.data.uk_gov_ons_geo.BoundariesType.CTRY_DEC_2023_UK_BFC,
    ],
)
def test_write_raw(boundaries_type, temp_dir):
    fryer.data.uk_gov_ons_geo.write_raw(
        boundaries_type=boundaries_type,
        path_log=temp_dir,
        path_data=temp_dir,
    )
    path = fryer.data.uk_gov_ons_geo.path_raw(
        boundaries_type=boundaries_type,
        path_data=temp_dir,
    )
    assert (path.parent / "complete").exists()


@pytest.mark.integration
def test_read_raw(temp_dir):
    gdf = fryer.data.uk_gov_ons_geo.read_raw(
        boundaries_type=fryer.data.uk_gov_ons_geo.BoundariesType.CTRY_DEC_2023_UK_BFC,
        path_log=temp_dir,
    )
    assert len(gdf) == 4
