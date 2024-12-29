import time
import requests
import datetime
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

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

# ----------------------------
# 2) DEFINITION OF EXECUTION PERIOD (in minutes)
# ----------------------------
execution_period = 1  # <--- adjust this to how many minutes you want

# ----------------------------
# 3) PERIODIC STATUS POLL (DYNAMIC)
# ----------------------------
STATUS_DATA_URL = (
    "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/"
    "status/oicp/ch.bfe.ladestellen-elektromobilitaet.json"
)

def get_station_id(conn, evse_id):
    """
    Look up 'id' in charging_stations for a given evse_id.
    If not found, insert a minimal record (evse_id) for it.
    """
    try:
        row = conn.execute(
            text("SELECT id FROM charging_stations WHERE evse_id = :evse_id"),
            {"evse_id": evse_id}
        ).fetchone()

        if row:
            return row[0]
        else:
            # Insert minimal row if not present
            result = conn.execute(
                text("INSERT INTO charging_stations (evse_id) VALUES (:evse_id)"),
                {"evse_id": evse_id}
            )
            return result.lastrowid

    except SQLAlchemyError as e:
        print(f"[{datetime.datetime.now()}] Database error in get_station_id: {str(e)}", file=sys.stderr)
        # Optionally re-raise if you want the script to stop on DB error
        raise

def poll_status():
    """
    Fetches dynamic statuses and inserts a row into charging_station_status_history
    for EVERY station on EVERY poll.
    """
    # 1) Fetch the data with basic HTTP error handling
    try:
        response = requests.get(STATUS_DATA_URL, timeout=10)
        response.raise_for_status()  # Raises HTTPError if status != 200
    except requests.RequestException as e:
        print(f"[{datetime.datetime.now()}] Error fetching data from {STATUS_DATA_URL}: {str(e)}", file=sys.stderr)
        return  # Skip this poll iteration

    # 2) Parse JSON
    try:
        data = response.json()
    except ValueError as e:
        print(f"[{datetime.datetime.now()}] Error parsing JSON from response: {str(e)}", file=sys.stderr)
        return  # Skip this poll iteration

    # 3) Insert into DB
    try:
        with engine.begin() as conn:
            # with engine.begin() auto-commits on success or rolls back on error
            # Check structure: data should have data["EVSEStatuses"]
            if "EVSEStatuses" not in data:
                print(f"[{datetime.datetime.now()}] JSON missing 'EVSEStatuses' key. Skipping.", file=sys.stderr)
                return

            for item in data["EVSEStatuses"]:
                # For safety, item should have "EVSEStatusRecord"
                if "EVSEStatusRecord" not in item:
                    print(f"[{datetime.datetime.now()}] JSON item missing 'EVSEStatusRecord'. Skipping item.", file=sys.stderr)
                    continue

                for record in item["EVSEStatusRecord"]:
                    evse_id = record.get("EvseID")
                    current_status = record.get("EVSEStatus")  # "Available", "Occupied", etc.

                    if not evse_id or not current_status:
                        print(f"[{datetime.datetime.now()}] Missing EvseID or EVSEStatus in record. Skipping.", file=sys.stderr)
                        continue

                    station_id = get_station_id(conn, evse_id)
                    conn.execute(
                        text("""
                            INSERT INTO charging_station_status_history (station_id, status)
                            VALUES (:station_id, :status)
                        """),
                        {"station_id": station_id, "status": current_status}
                    )

        print(f"[{datetime.datetime.now()}] Polled status for all stations.")
    except SQLAlchemyError as e:
        print(f"[{datetime.datetime.now()}] Database error during poll_status inserts: {str(e)}", file=sys.stderr)
        # We can continue or re-raise depending on desired behavior
        # raise

def main():
    """
    Polls every minute until 'execution_period' minutes have passed.
    'execution_period' is defined at the top of the script.
    """
    start_time = datetime.datetime.now()
    while True:
        poll_status()

        # Sleep for 1 minute
        time.sleep(60)

        # Check elapsed time in seconds
        elapsed_seconds = (datetime.datetime.now() - start_time).total_seconds()
        if elapsed_seconds >= execution_period * 60:
            print(f"Reached execution period of {execution_period} minutes. Exiting.")
            break

if __name__ == "__main__":
    main()
