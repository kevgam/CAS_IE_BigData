import datetime
import requests
from sqlalchemy import create_engine, text

# ----------------------------
# 1) DATABASE CONNECTION PARAMS
# ----------------------------
host = 'localhost'
port = 3306
user = 'root'
password = ''  # or your actual MySQL password
database = 'CAS_IE_Big_Data'

engine = create_engine(
    f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
)

print("Engine URL:", engine.url)

# ----------------------------
# 2) STATIC ENDPOINT + UPSERT
# ----------------------------
STATIC_DATA_URL = (
    "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/"
    "data/oicp/ch.bfe.ladestellen-elektromobilitaet.json"
)

def upsert_station(conn, evse_record):
    """
    Insert/Update a station in charging_stations based on 'evse_id' (unique).
    Pulls metadata like station_name, address, lat/long, last_update from
    the static record.
    """
    evse_id = evse_record.get("EvseID")

    # Station name
    station_name = None
    if evse_record.get("ChargingStationNames"):
        station_name = evse_record["ChargingStationNames"][0].get("value")

    # Build an address string
    address_data = evse_record.get("Address", {})
    address_str = f"{address_data.get('Street','')}, {address_data.get('PostalCode','')} {address_data.get('City','')}"

    # Coordinates
    coords = evse_record.get("GeoCoordinates", {}).get("Google", "")
    lat, lon = None, None
    if coords and coords != "None None":
        try:
            lat_str, lon_str = coords.split()
            lat, lon = float(lat_str), float(lon_str)
        except:
            pass

    # Last update timestamp
    last_update_str = evse_record.get("lastUpdate")
    last_update = None
    if last_update_str:
        # e.g. "2024-12-29T03:15:03.627Z" -> Python datetime
        last_update = datetime.datetime.fromisoformat(last_update_str.replace("Z",""))

    # Perform an upsert with MySQL's INSERT ... ON DUPLICATE KEY UPDATE
    conn.execute(
        text("""
            INSERT INTO charging_stations (
                evse_id, station_name, address, latitude, longitude, last_update
            )
            VALUES (:evse_id, :station_name, :address, :latitude, :longitude, :last_update)
            ON DUPLICATE KEY UPDATE
                station_name = VALUES(station_name),
                address = VALUES(address),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                last_update = VALUES(last_update)
        """),
        {
            "evse_id": evse_id,
            "station_name": station_name,
            "address": address_str,
            "latitude": lat,
            "longitude": lon,
            "last_update": last_update
        }
    )
    row = conn.execute(text("SELECT COUNT(*) FROM charging_stations")).fetchone()
    print("Row count in charging_stations:", row[0])


def initial_load_static_data():
    """
    Fetches station data from the static endpoint and upserts every station
    into charging_stations.
    """
    response = requests.get(STATIC_DATA_URL)
    data = response.json()

    # data["EVSEData"][0]["EVSEDataRecord"] -> list of station objects
    records = data["EVSEData"][0]["EVSEDataRecord"]

    with engine.begin() as conn:
        for record in records:
            upsert_station(conn, record)

    print(f"[{datetime.datetime.now()}] Initial static data load completed. "
          f"Stations processed: {len(records)}")


def main():
    # Step 1: Check if the table is empty
    with engine.begin() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM charging_stations")).fetchone()
        station_count = result[0] if result else 0

    # Step 2: If empty, load static data
    if station_count == 0:
        initial_load_static_data()
    else:
        print(f"[{datetime.datetime.now()}] charging_stations already has data; skipping initial load.")

if __name__ == "__main__":
    main()
