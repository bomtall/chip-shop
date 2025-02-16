import folium

__all__ = ["create", "london", "uk"]


def create(
    latitude: float,
    longitude: float,
    zoom_start: int,
) -> folium.Map:
    """Create a folium map object with the given latitude, longitude and zoom."""
    return folium.Map(location=[latitude, longitude], zoom_start=zoom_start)


def uk(
    latitude: float = 55.3784,
    longitude: float = -3.4360,
    zoom_start: int = 6,
) -> folium.Map:
    """Create a folium map object with the given latitude, longitude and zoom. Defaults to view of UK."""
    return create(latitude=latitude, longitude=longitude, zoom_start=zoom_start)


def london(
    latitude: float = 51.5098,
    longitude: float = -0.1181,
    zoom_start: int = 11,
) -> folium.Map:
    """Create a folium map object with the given latitude, longitude and zoom. Defaults to view of London."""
    return create(latitude=latitude, longitude=longitude, zoom_start=zoom_start)
