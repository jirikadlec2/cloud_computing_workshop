from datetime import datetime
import os
import json
import io
import boto3
from matplotlib import pyplot as plt
import pandas as pd
from pystac_client import Client
from odc.stac import configure_rio, stac_load
import requests
from shapely.geometry import box
from matplotlib.colors import ListedColormap

# For local test, read from env vars
# Later pass these via command-line
LAKE_NAME = os.environ.get("LAKE_NAME", "Unknown_Lake")
LAKE_ID = os.environ.get("LAKE_ID", "0")

DATASET_NAME = os.environ.get("DATASET_NAME", "gm_s2_rolling") # wofs_ls_summary_annual or gm_s2_rolling

LAKE_NORTH = os.environ.get("LAKE_NORTH", 14.5)
LAKE_SOUTH = os.environ.get("LAKE_SOUTH", 12.5)
LAKE_EAST = os.environ.get("LAKE_EAST", 15.1)
LAKE_WEST = os.environ.get("LAKE_WEST", 13.1)

BBOX = json.loads(os.environ.get("BBOX", f"[{LAKE_WEST}, {LAKE_SOUTH}, {LAKE_EAST}, {LAKE_NORTH}]"))

START_DATE = os.environ.get("START_DATE", "2019-01-01")
END_DATE = os.environ.get("END_DATE", datetime.today().strftime("%Y-%m-%d"))
S3_BUCKET = os.environ.get("S3_BUCKET", "cloud-computing-workshop-2025")


def guess_country(bbox):
    lat = (bbox[1] + bbox[3]) / 2
    lon = (bbox[0] + bbox[2]) / 2
    params = {
        "lat": lat,
        "lon": lon,
        "format": "json",
        "accept-language": "en"  # force English output
    }
    url = "https://nominatim.openstreetmap.org/reverse"
    res = requests.get(url, headers={'User-Agent': 'geo-app'}, params=params)
    data = res.json()
    return data.get('address', {}).get('country', 'unknown')


def process_lake(country, lake_id, lake_name, bbox, start_date, end_date):
    configure_rio(cloud_defaults=True, aws={"aws_unsigned": True}, AWS_S3_ENDPOINT="s3.af-south-1.amazonaws.com")
    catalog = Client.open("https://explorer.digitalearth.africa/stac")
    query = catalog.search(bbox=bbox, collections=["gm_s2_rolling"], datetime=f"{start_date}/{end_date}")
    items = list(query.items())
    if not items:
        print(f"No data found for {lake_name}")
        return
    print(f"Found {len(items)} STAC items for {lake_name}.")

    crs = "EPSG:6933"
    resolution = 100
    ds = stac_load(items, bands=("B03", "B11"), crs=crs, resolution=resolution, groupby="solar_day", bbox=bbox)

    ds["MNDWI"] = (ds.B03 - ds.B11) / (ds.B03 + ds.B11)
    water_mask = ds["MNDWI"] > 0.5
    pixel_area_sq_km = (resolution**2) / 1_000_000.0
    water_area_series = (water_mask.sum(dim=["x","y"]) * pixel_area_sq_km).to_pandas()

    csv_buffer = io.StringIO()
    water_area_series.to_csv(csv_buffer)
    #print(f"--- CSV OUTPUT for {lake_name} ---")
    #print(csv_buffer.getvalue())

    # Save the CSV to S3 (public object, anyone can read) and generate public URL
    s3 = boto3.client("s3", region_name="af-south-1")
    lake_name_shortened = lake_name.replace(" ", "_")
    lake_name_shortened = f"{lake_name_shortened}_{lake_id}" if lake_id else lake_name_shortened
    s3.put_object(Bucket=S3_BUCKET, Key=f"output/{lake_name_shortened}_water_area.csv",
                  Body=csv_buffer.getvalue(), ContentType="text/csv",
                  ACL="public-read")
    print(f"Saved water area data for {lake_name} to S3 bucket {S3_BUCKET}.")
    output_url = f"https://{S3_BUCKET}.s3.af-south-1.amazonaws.com/output/{country}/{lake_name_shortened}_water_area.csv"
    return output_url

def process_lake_wofs(country, lake_id, lake_name, bbox, start_date, end_date):
    configure_rio(cloud_defaults=True, aws={"aws_unsigned": True}, AWS_S3_ENDPOINT="s3.af-south-1.amazonaws.com")
    catalog = Client.open("https://explorer.digitalearth.africa/stac")
    query = catalog.search(bbox=bbox, collections=["wofs_ls_summary_annual"], datetime=f"{start_date}/{end_date}")
    items = list(query.items())
    if not items:
        print(f"No data found for {lake_name}")
        return
    print(f"Found {len(items)} STAC items for {lake_name}.")

    crs = "EPSG:6933"
    resolution = 200
    ds = stac_load(
        items,
        bands=["count_wet","count_clear"],
        crs=crs,
        resolution=resolution,
        chunks={},
        groupby="solar_day",
        bbox=bbox,
    )
    
    # Compute the percent of valid wet pixels
    ds["percent_wet"] = (ds.count_wet / ds.count_clear) * 100.0
    percent_wet = ds.percent_wet.compute()

    # Mask and save to figure
    cmap_custom = ListedColormap(['white', 'blue'])

    # Save this plot to image and upload to S3
    #fig, ax = plt.subplots(figsize=(12, 6))
    lake_name_shortened = f"{lake_name.replace(" ", "_")}_{lake_id}" if lake_id else lake_name.replace(" ", "_")
    percent_wet.plot(col="time", col_wrap=6, cmap='YlGnBu')
    plt.title(f"Percent Wet Area - {lake_name}")
    plt.savefig(f"/tmp/{lake_name_shortened}_percent_wet.png")
    plt.close()

    s3 = boto3.client("s3", region_name="af-south-1")

    with open(f"/tmp/{lake_name_shortened}_percent_wet.png", "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"output/{country}/{lake_name_shortened}_percent_wet.png",
            Body=f,
            ContentType="image/png",
            ACL="public-read"
        )
    
    # Define the percent_wet to identify permanent water
    permanent_water_threshold = 80
    seasonal_water_threshold = 40

    # Calculate the area of a single pixel in square kilometers
    pixel_area_sq_m = resolution * resolution
    pixel_area_sq_km = pixel_area_sq_m / 1000000.0

    # Create a "permanent water mask (wet for 90% of the time or more)"
    permanent_water_mask = percent_wet > permanent_water_threshold

    # Count the number of water pixels for each time step
    permanent_water_pixel_count = permanent_water_mask.sum(dim=["x", "y"])

    # Calculate the total water area in square kilometers for each time step
    permanent_water_area_sq_km = permanent_water_pixel_count * pixel_area_sq_km

    # Convert the xarray DataArray to a pandas Series for plotting
    permanent_water_area_series = permanent_water_area_sq_km.compute().to_pandas()

    # Same for seasonal water flooding
    seasonal_water_mask = percent_wet > seasonal_water_threshold
    seasonal_water_pixel_count = seasonal_water_mask.sum(dim=["x", "y"])
    seasonal_water_area_sq_km = seasonal_water_pixel_count * pixel_area_sq_km

    # Convert the xarray DataArray to a pandas Series for plotting
    seasonal_water_area_series = seasonal_water_area_sq_km.compute().to_pandas()

    # Save the time series plot below to image and upload that image to S3
    # Plot the time series of water area
    plt.figure(figsize=(12, 6))

    # Plot the permanent water series with a label
    permanent_water_area_series.plot(style='-o', label=f'Permanent Water (>{permanent_water_threshold}%)')

    # Plot the seasonal water series with a label on the same chart
    seasonal_water_area_series.plot(style='-x', label=f'Seasonal Water (>{seasonal_water_threshold}%)')

    plt.title(f"Time Series of Water Area for Lake {lake_name}, {country}")
    plt.xlabel("Date")
    plt.ylabel("Water Area ($km^2$)")
    plt.grid(True)
    plt.legend()  # Add this line to display the legend
    plt.show()
    # save plot to image and upload to S3
    plt.savefig(f"/tmp/{lake_name_shortened}_water_area_time_series.png")
    plt.close()
    
    with open(f"/tmp/{lake_name_shortened}_water_area_time_series.png", "rb") as f:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"output/{country}/{lake_name_shortened}_water_area_time_series.png",
            Body=f,
            ContentType="image/png",
            ACL="public-read"
        )

    # You can save both series to a single CSV file if you want
    combined_series = pd.DataFrame({
        'permanent_water': permanent_water_area_series,
        'seasonal_water': seasonal_water_area_series
    })
    combined_series.to_csv("water_area_time_series.csv")


    csv_buffer = io.StringIO()
    combined_series.to_csv(csv_buffer)
    #print(f"--- CSV OUTPUT for {lake_name} ---")
    #print(csv_buffer.getvalue())

    # Save the CSV to S3 (public object, anyone can read) and generate public URL
    s3.put_object(Bucket=S3_BUCKET, Key=f"output/{country}/{lake_name_shortened}_water_area.csv",
                  Body=csv_buffer.getvalue(), ContentType="text/csv",
                  ACL="public-read")
    print(f"Saved water area data for {lake_name} to S3 bucket {S3_BUCKET}.")
    output_url = f"https://{S3_BUCKET}.s3.af-south-1.amazonaws.com/output/{country}/{lake_name_shortened}_water_area.csv"
    return output_url


if __name__ == "__main__":
    print(f"Processing waterbody: {LAKE_NAME} with BBOX: {BBOX} and time range: {START_DATE} to {END_DATE} with dataset {DATASET_NAME}")
    start_time = datetime.now()

    country = guess_country(BBOX)
    if DATASET_NAME == "wofs_ls_summary_annual":
        output_url = process_lake_wofs(country, LAKE_ID, LAKE_NAME, BBOX, START_DATE, END_DATE)
    else:
        output_url = process_lake(country, LAKE_ID, LAKE_NAME, BBOX, START_DATE, END_DATE)
    print(f"Finished processing waterbody: {LAKE_NAME}, time taken: {datetime.now() - start_time}")
    print(f"Results saved to: {output_url}")
