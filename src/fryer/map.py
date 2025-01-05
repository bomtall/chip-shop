import folium


__all__ = ["create"]


def create(
    latitude: float = 55.3784, longitude: float = -3.4360, zoom_start: int = 6
) -> folium.Map:
    """
    Create a folium map object with the given latitude, longitude and zoom. Defaults to view of UK.
    """
    return folium.Map(location=[latitude, longitude], zoom_start=zoom_start)
