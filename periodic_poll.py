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
execution_period = 1440  # <--- adjust this to how many minutes you want

# ----------------------------
# 3) PERIODIC STATUS POLL (DYNAMIC)
# ----------------------------
STATUS_DATA_URL = (
    "https://data.geo.admin.ch/ch.bfe.ladestellen-elektromobilitaet/"
    "status/oicp/ch.bfe.ladestellen-elektromobilitaet.json"
)

def poll_status():
    """
    Fetches dynamic statuses and inserts a row into charging_station_status_history
    for every station on every poll.
    """
    try:
        # 1) Fetch the data with basic HTTP error handling
        response = requests.get(STATUS_DATA_URL, timeout=10)
        response.raise_for_status()  # Raises HTTPError if status != 200
    except requests.RequestException as e:
        print(f"[{datetime.datetime.now()}] Error fetching data from {STATUS_DATA_URL}: {str(e)}", file=sys.stderr)
        return  # Skip this poll iteration

    try:
        # 2) Parse JSON
        data = response.json()
    except ValueError as e:
        print(f"[{datetime.datetime.now()}] Error parsing JSON from response: {str(e)}", file=sys.stderr)
        return  # Skip this poll iteration

    try:
        with engine.begin() as conn:
            # Ensure "EVSEStatuses" key exists in the response
            if "EVSEStatuses" not in data:
                print(f"[{datetime.datetime.now()}] JSON missing 'EVSEStatuses' key. Skipping.", file=sys.stderr)
                return

            for item in data["EVSEStatuses"]:
                if "EVSEStatusRecord" not in item:
                    print(f"[{datetime.datetime.now()}] JSON item missing 'EVSEStatusRecord'. Skipping item.", file=sys.stderr)
                    continue

                for record in item["EVSEStatusRecord"]:
                    evse_id = record.get("EvseID")
                    current_status = record.get("EVSEStatus")

                    if not evse_id or not current_status:
                        print(f"[{datetime.datetime.now()}] Missing EvseID or EVSEStatus in record. Skipping.", file=sys.stderr)
                        continue

                    # Ensure the evse_id exists in charging_stations
                    conn.execute(
                        text("""
                            INSERT IGNORE INTO charging_stations (evse_id)
                            VALUES (:evse_id)
                        """),
                        {"evse_id": evse_id}
                    )

                    # Insert the status update into charging_station_status_history
                    conn.execute(
                        text("""
                            INSERT INTO charging_station_status_history (evse_id, status, polled_at)
                            VALUES (:evse_id, :status, CURRENT_TIMESTAMP)
                        """),
                        {"evse_id": evse_id, "status": current_status}
                    )

        print(f"[{datetime.datetime.now()}] Polled status for all stations.")
    except SQLAlchemyError as e:
        print(f"[{datetime.datetime.now()}] Database error during poll_status inserts: {str(e)}", file=sys.stderr)


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
