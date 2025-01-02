import pandas as pd
from sqlalchemy import create_engine
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
# 2) EXPORT FUNCTION
# ----------------------------
def export_table_to_csv(table_name, output_path):
    """
    Export a table from the database to a CSV file.

    :param table_name: Name of the table to export.
    :param output_path: Path where the CSV file will be saved.
    """
    try:
        with engine.connect() as conn:
            # Fetch data from the specified table
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, conn)

            # Save the dataframe as a CSV file with semicolon separator
            df.to_csv(output_path, index=False, sep=';')

            print(f"Table '{table_name}' has been successfully exported to '{output_path}'")

    except SQLAlchemyError as e:
        print(f"Database error: {str(e)}")
    except Exception as e:
        print(f"Error: {str(e)}")


# ----------------------------
# 3) EXPORT CONFIG
# ----------------------------
if __name__ == "__main__":
    # Define the table name and output path
    table_name = 'CAS_IE_Big_Data.charging_station_status_history'
    output_path = '/Users/kevingamuzza/Documents/Privat/ZHAW CAS Information Engineering/Big Data/Leistungsnachweis/charging_station_status_history_new.csv'

    # Export the table
    export_table_to_csv(table_name, output_path)
