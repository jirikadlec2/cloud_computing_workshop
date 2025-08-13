import stackstac
import pystac_client

URL = "https://explorer.digitalearth.africa/stac"
catalog = pystac_client.Client.open(URL)

stac_items = catalog.search(
    intersects=dict(type="Point", coordinates=[13.8, 13.4]),
    collections=["gm_s2_rolling"],
    datetime="2025-01-01/2025-07-01"
).get_all_items()

stack = stackstac.stack(stac_items, epsg=6933)
print(stack)

catalog = pystac_client.Client.open(...)
query = catalog.search(...)
xx = odc.stac.load(
    query.items(),
    bands=["red", "green", "blue"],
)
xx.red.plot.imshow(col="time")