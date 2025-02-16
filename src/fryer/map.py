from collections.abc import Iterable

import folium
import polars as pl
from folium.plugins import FastMarkerCluster

__all__ = [
    "create",
    "london",
    "make_markers",
    "uk",
]


def create(
    latitude: float,
    longitude: float,
    zoom_start: int,
) -> folium.Map:
    """Create a folium map object with the given latitude, longitude and zoom."""
    return folium.Map(
        location=[latitude, longitude],
        zoom_start=zoom_start,
    )


def uk(
    latitude: float = 55.3784,
    longitude: float = -3.4360,
    zoom_start: int = 6,
) -> folium.Map:
    """Create a folium map object with the given latitude, longitude and zoom. Defaults to view of UK."""
    return create(
        latitude=latitude,
        longitude=longitude,
        zoom_start=zoom_start,
    )


def london(
    latitude: float = 51.5098,
    longitude: float = -0.1181,
    zoom_start: int = 11,
) -> folium.Map:
    """Create a folium map object with the given latitude, longitude and zoom. Defaults to view of London."""
    return create(
        latitude=latitude,
        longitude=longitude,
        zoom_start=zoom_start,
    )


def make_markers(  # noqa: C901, PLR0912, PLR0913
    df: pl.DataFrame | pl.LazyFrame,
    score: str | None = None,
    color: str | None = None,
    tooltip: Iterable[str] | str | None = None,
    popup: Iterable[str] | str | None = None,
    icon_create_function: str | None = None,
    cluster_threshold: tuple[float, float] | None = None,
    max_cluster_radius: int = 80,
    add_to_map: folium.Map | None = None,
) -> FastMarkerCluster:
    if isinstance(df, pl.DataFrame):
        df = df.lazy()
    options = {"maxClusterRadius": max_cluster_radius}

    index = 2
    cols = []
    if score is not None:
        cols.append(score)
        score_marker_options = f", {{score: row[{index}]}}"
        index += 1
        if icon_create_function is None:
            if cluster_threshold is None:
                message = f"{cluster_threshold=} cannot be None"
                raise ValueError(message)
            icon_create_function = f"""
function(cluster) {{
    var markers = cluster.getAllChildMarkers();
    var sum = 0;
    var count = 0;
    for (var i = 0; i < markers.length; i++) {{
        if (markers[i].options["score"] > 0) {{
            sum += markers[i].options["score"];
            count += 1
        }}
    }}
    var avg = 0;
    if (count != 0) {{
        avg = sum / count;
    }}

    var c = ' marker-cluster-';
    if (avg < {cluster_threshold[0]}) {{
        c += 'large';
    }} else if (avg <= {cluster_threshold[1]}) {{
        c += 'medium';
    }} else {{
        c += 'small';
    }}

    return L.divIcon({{
        html: '<div><span>' + avg.toFixed(2) + '</span></div>',
        className: 'marker-cluster' + c,
        iconSize: new L.Point(40, 40)
    }});
}}
"""

    else:
        score_marker_options = ""

    if color is not None:
        cols.append(color)
        color_marker_options = f", markerColor: row[{index}]"
        index += 1
    else:
        color_marker_options = ""

    if tooltip is not None:
        if isinstance(tooltip, Iterable) and not isinstance(tooltip, str):
            expression = pl.concat_str(
                [f"{column}: " + pl.col(column) for column in tooltip], separator="<br>"
            ).alias("__tooltip")
            df = df.with_columns(expression)
            tooltip = "__tooltip"
        cols.append(tooltip)
        tooltip_marker_options = f"""
    var tooltip = L.tooltip();
    var tooltip_text = $(`<div id='mytext' class='display_text' style='width: 100.0%; height: 100.0%;'> ${{row[{index}]}}</div>`)[0];
    tooltip.setContent(tooltip_text);
    marker.bindTooltip(tooltip);
"""
        index += 1
    else:
        tooltip_marker_options = ""
    if popup is not None:
        if isinstance(popup, Iterable) and not isinstance(popup, str):
            expression = pl.concat_str(
                [f"{column}: " + pl.col(column) for column in popup], separator="<br>"
            ).alias("__popup")
            df = df.with_columns(expression)
            popup = "__popup"
        cols.append(popup)
        popup_marker_options = f"""
    var popup = L.popup({{maxWidth: '300'}});
    var popup_text = $(`<div id='mytext' class='display_text' style='width: 100.0%; height: 100.0%;'> ${{row[{index}]}}</div>`)[0];
    popup.setContent(popup_text);
    marker.bindPopup(popup);
"""
        index += 1
    else:
        popup_marker_options = ""

    fast_marker_cluster = FastMarkerCluster(
        data=df.select(["latitude", "longitude", *cols]).collect().rows(),
        # https://github.com/Leaflet/Leaflet.markercluster/blob/15ed12654acdc54a4521789c498e4603fe4bf781/src/MarkerClusterGroup.js
        options=options,
        callback=f"""
function (row) {{
    var icon, marker;
    icon = L.AwesomeMarkers.icon({{icon: "map-marker"{color_marker_options}}});
    marker = L.marker(new L.LatLng(row[0], row[1]){score_marker_options});
    marker.setIcon(icon);
    // add tooltip if needed
{tooltip_marker_options}
    // add popup
{popup_marker_options}
    return marker;
}};
""",
        icon_create_function=icon_create_function,
    )

    if add_to_map is not None:
        fast_marker_cluster.add_to(add_to_map)
    return fast_marker_cluster
