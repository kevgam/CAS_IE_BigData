import datetime
import requests
from sqlalchemy import create_engine, text
import json  # Ensure json is imported for serializing fields

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

    # Extract fields
    charging_station_id = evse_record.get("ChargingStationId")
    accessibility_location = evse_record.get("AccessibilityLocation")

    # Station name
    station_name = None
    if evse_record.get("ChargingStationNames"):
        station_name = evse_record["ChargingStationNames"][0].get("value")

    # Address details
    address_data = evse_record.get("Address", {})
    address_str = f"{address_data.get('Street', '')}, {address_data.get('PostalCode', '')} {address_data.get('City', '')}"
    address_house_num = address_data.get("HouseNum")
    address_street = address_data.get("Street")
    address_city = address_data.get("City")
    address_region = address_data.get("Region")
    address_postal_code = address_data.get("PostalCode")
    address_country = address_data.get("Country")
    address_time_zone = address_data.get("TimeZone")

    # Coordinates
    coords = evse_record.get("GeoCoordinates", {}).get("Google", "")
    lat, lon = None, None
    if coords and coords != "None None":
        try:
            lat_str, lon_str = coords.split()
            lat, lon = float(lat_str), float(lon_str)
        except ValueError:
            pass

    # Other fields
    def serialize(field):
        return json.dumps(field) if field else None

    authentication_modes = serialize(evse_record.get("AuthenticationModes"))
    calibration_law_data_availability = evse_record.get("CalibrationLawDataAvailability")
    charging_facilities = serialize(evse_record.get("ChargingFacilities"))
    dynamic_info_available = evse_record.get("DynamicInfoAvailable")
    hotline_phone_number = evse_record.get("HotlinePhoneNumber")
    hub_operator_id = evse_record.get("HubOperatorID")
    is_hubject_compatible = evse_record.get("IsHubjectCompatible")
    is_open_24_hours = evse_record.get("IsOpen24Hours")
    payment_options = serialize(evse_record.get("PaymentOptions"))
    plugs = serialize(evse_record.get("Plugs"))
    renewable_energy = evse_record.get("RenewableEnergy")
    value_added_services = serialize(evse_record.get("ValueAddedServices"))
    delta_type = evse_record.get("deltaType")
    accessibility = evse_record.get("Accessibility")
    geo_charging_point_entrance = evse_record.get("GeoChargingPointEntrance", {}).get("Google")
    clearinghouse_id = evse_record.get("ClearinghouseID")
    opening_times = serialize(evse_record.get("OpeningTimes"))
    charging_station_location_reference = serialize(evse_record.get("ChargingStationLocationReference"))
    energy_source = serialize(evse_record.get("EnergySource"))
    environmental_impact = serialize(evse_record.get("EnvironmentalImpact"))
    location_image = serialize(evse_record.get("LocationImage"))
    suboperator_name = evse_record.get("SuboperatorName")
    max_capacity = evse_record.get("MaxCapacity")
    additional_info = evse_record.get("AdditionalInfo")
    charging_pool_id = evse_record.get("ChargingPoolID")
    dynamic_power_level = evse_record.get("DynamicPowerLevel")
    hardware_manufacturer = evse_record.get("HardwareManufacturer")

    # Last update timestamp
    last_update_str = evse_record.get("lastUpdate")
    last_update = None
    if last_update_str:
        last_update = datetime.datetime.fromisoformat(last_update_str.replace("Z", ""))

    # Perform an upsert with MySQL's INSERT ... ON DUPLICATE KEY UPDATE
    conn.execute(
        text("""
            INSERT INTO charging_stations (
                evse_id, charging_station_id, accessibility_location, station_name, address, 
                address_house_num, address_street, address_city, address_region, 
                address_postal_code, address_country, address_time_zone, authentication_modes, 
                calibration_law_data_availability, charging_facilities, dynamic_info_available, 
                hotline_phone_number, hub_operator_id, is_hubject_compatible, is_open_24_hours, 
                payment_options, plugs, renewable_energy, value_added_services, delta_type, 
                accessibility, geo_coordinates, geo_charging_point_entrance, clearinghouse_id, 
                opening_times, charging_station_location_reference, energy_source, 
                environmental_impact, location_image, suboperator_name, max_capacity, 
                additional_info, charging_pool_id, dynamic_power_level, hardware_manufacturer, 
                latitude, longitude, last_update
            )
            VALUES (
                :evse_id, :charging_station_id, :accessibility_location, :station_name, :address, 
                :address_house_num, :address_street, :address_city, :address_region, 
                :address_postal_code, :address_country, :address_time_zone, :authentication_modes, 
                :calibration_law_data_availability, :charging_facilities, :dynamic_info_available, 
                :hotline_phone_number, :hub_operator_id, :is_hubject_compatible, :is_open_24_hours, 
                :payment_options, :plugs, :renewable_energy, :value_added_services, :delta_type, 
                :accessibility, :geo_coordinates, :geo_charging_point_entrance, :clearinghouse_id, 
                :opening_times, :charging_station_location_reference, :energy_source, 
                :environmental_impact, :location_image, :suboperator_name, :max_capacity, 
                :additional_info, :charging_pool_id, :dynamic_power_level, :hardware_manufacturer, 
                :latitude, :longitude, :last_update
            )
            ON DUPLICATE KEY UPDATE
                charging_station_id = VALUES(charging_station_id),
                accessibility_location = VALUES(accessibility_location),
                station_name = VALUES(station_name),
                address = VALUES(address),
                address_house_num = VALUES(address_house_num),
                address_street = VALUES(address_street),
                address_city = VALUES(address_city),
                address_region = VALUES(address_region),
                address_postal_code = VALUES(address_postal_code),
                address_country = VALUES(address_country),
                address_time_zone = VALUES(address_time_zone),
                authentication_modes = VALUES(authentication_modes),
                calibration_law_data_availability = VALUES(calibration_law_data_availability),
                charging_facilities = VALUES(charging_facilities),
                dynamic_info_available = VALUES(dynamic_info_available),
                hotline_phone_number = VALUES(hotline_phone_number),
                hub_operator_id = VALUES(hub_operator_id),
                is_hubject_compatible = VALUES(is_hubject_compatible),
                is_open_24_hours = VALUES(is_open_24_hours),
                payment_options = VALUES(payment_options),
                plugs = VALUES(plugs),
                renewable_energy = VALUES(renewable_energy),
                value_added_services = VALUES(value_added_services),
                delta_type = VALUES(delta_type),
                accessibility = VALUES(accessibility),
                geo_coordinates = VALUES(geo_coordinates),
                geo_charging_point_entrance = VALUES(geo_charging_point_entrance),
                clearinghouse_id = VALUES(clearinghouse_id),
                opening_times = VALUES(opening_times),
                charging_station_location_reference = VALUES(charging_station_location_reference),
                energy_source = VALUES(energy_source),
                environmental_impact = VALUES(environmental_impact),
                location_image = VALUES(location_image),
                suboperator_name = VALUES(suboperator_name),
                max_capacity = VALUES(max_capacity),
                additional_info = VALUES(additional_info),
                charging_pool_id = VALUES(charging_pool_id),
                dynamic_power_level = VALUES(dynamic_power_level),
                hardware_manufacturer = VALUES(hardware_manufacturer),
                latitude = VALUES(latitude),
                longitude = VALUES(longitude),
                last_update = VALUES(last_update)
        """),
        {
            "evse_id": evse_id,
            "charging_station_id": charging_station_id,
            "accessibility_location": accessibility_location,
            "station_name": station_name,
            "address": address_str,
            "address_house_num": address_house_num,
            "address_street": address_street,
            "address_city": address_city,
            "address_region": address_region,
            "address_postal_code": address_postal_code,
            "address_country": address_country,
            "address_time_zone": address_time_zone,
            "authentication_modes": authentication_modes,
            "calibration_law_data_availability": calibration_law_data_availability,
            "charging_facilities": charging_facilities,
            "dynamic_info_available": dynamic_info_available,
            "hotline_phone_number": hotline_phone_number,
            "hub_operator_id": hub_operator_id,
            "is_hubject_compatible": is_hubject_compatible,
            "is_open_24_hours": is_open_24_hours,
            "payment_options": payment_options,
            "plugs": plugs,
            "renewable_energy": renewable_energy,
            "value_added_services": value_added_services,
            "delta_type": delta_type,
            "accessibility": accessibility,
            "geo_coordinates": coords,
            "geo_charging_point_entrance": geo_charging_point_entrance,
            "clearinghouse_id": clearinghouse_id,
            "opening_times": opening_times,
            "charging_station_location_reference": charging_station_location_reference,
            "energy_source": energy_source,
            "environmental_impact": environmental_impact,
            "location_image": location_image,
            "suboperator_name": suboperator_name,
            "max_capacity": max_capacity,
            "additional_info": additional_info,
            "charging_pool_id": charging_pool_id,
            "dynamic_power_level": dynamic_power_level,
            "hardware_manufacturer": hardware_manufacturer,
            "latitude": lat,
            "longitude": lon,
            "last_update": last_update,
        }
    )


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
