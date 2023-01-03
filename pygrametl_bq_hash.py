import hashlib
import logging
import os

import psycopg2
from google.cloud.bigquery import dbapi
from pygrametl.datasources import CSVSource, SQLSource
from pygrametl.tables import CachedDimension, FactTable

import pygrametl
from bq_defs import *


def create_psql_conn(database_name, user, password="", host="", port="5432"):
    psql_conn = psycopg2.connect(
        database=database_name, user=user, host=host, port=port
    )

    return psql_conn


# Pygrametl uses a database connection following PEP 249 specification.
# For BQ can use the following:
# - https://cloud.google.com/python/docs/reference/bigquery/latest/dbapi#googlecloudbigquerydbapiconnectclientnone-bqstorageclientnone
def bq_connector(bigquery_client):
    # bq_connection = dbapi.Connection(client=bigquery_client)
    bq_connection = dbapi.connect(client=bigquery_client)

    return bq_connection


def hash_book(row):
    concat = row["title"] + row["genre"]
    hash = hashlib.sha256(concat.encode()).hexdigest()

    return hash


def hash_date(row):
    str_date = str(row["date"])
    hash = hashlib.sha256(str_date.encode()).hexdigest()

    return hash


def pygrametl_tut(gcp_configuration, bigquery_connection):
    toml_config = get_toml_file(f"{os.getcwd()}/config/")
    ext_db_info = toml_config["postgresql"]["eddylim"]

    # Opening connection to local PSQL database.
    ext_db_conn = create_psql_conn(
        database_name=ext_db_info["dbname"],
        user=ext_db_info["user"],
        host=ext_db_info["host"],
    )

    # Creation of connection wrapper.
    connection_wrapper = pygrametl.ConnectionWrapper(connection=bigquery_connection)

    # Defining the Dimension and Fact tables.
    dim_book = CachedDimension(
        name="pygrametl_hash.book",
        key="bookid",
        # attributes=["book", "genre"],
        attributes=["title", "genre", "book_hash_id"],
    )

    dim_time = CachedDimension(
        name="pygrametl_hash.time",
        key="timeid",
        attributes=["day", "month", "year", "time_hash_id"],
    )

    dim_location = CachedDimension(
        name="pygrametl_hash.location",
        key="locationid",
        attributes=["city", "region"],
        lookupatts=["city"],
    )

    fact_table = FactTable(
        name="pygrametl_hash.facttable",
        keyrefs=["book_hash_id", "locationid", "time_hash_id"],
        measures=["sale"],
    )

    logging.info("Inserting values into 'dim_location' table...")

    # Fill the Dimension Location with all the lines from 'region.csv' file.
    with open(
        f"{os.getcwd()}/pygrametl/beginner_guide_data/region.csv", "r", 16384
    ) as csv_region_file:
        csv_region_parsed = CSVSource(f=csv_region_file, delimiter=",")

        [dim_location.insert(row) for row in csv_region_parsed]

    logging.info("Inserted all values into 'dim_location' table.")

    result_columns = "title", "genre", "city", "date", "sale"

    # query = "select book, genre, store, date, sale from sale"
    query = "select book as title, genre, store, date, sale from sale limit 25"
    external_source = SQLSource(
        connection=ext_db_conn, query=query, names=result_columns
    )

    # print("Checking existing bookid and timeid and filling the Dim and Fact tables...")
    logging.info(
        "Checking existing bookid and timeid and filling the Dim tables and Fact table..."
    )
    for row in external_source:
        split_date(row)

        # Setting a new hash for the hashed column ids.
        row["book_hash_id"] = hash_book(row)
        row["time_hash_id"] = hash_date(row)

        # Lookup the given row. If that fails, insert it.
        # If found, see if values for attributes in otherrefs or
        #  measures have changed and update the found row if necessary (note that values for attributes in keyrefs are not allowed to change).
        # docs: http://chrthomsen.github.io/pygrametl/doc/api/tables.html#pygrametl.tables.Dimension.ensure

        # So checks if the row exists in both dim_book and dim_time, if it doesn't exist create new id when it does exist use existing id.
        row["bookid"] = dim_book.ensure(row)
        row["timeid"] = dim_time.ensure(row)

        # print(f"Book ID: {row['bookid']}")
        # print(f"Time ID: {row['timeid']}")
        # print(row, "\n")
        logging.info(row)
        # print("\n")

        # The location dimension is pre-filled, so a missing row is an error.
        row["locationid"] = dim_location.lookup(row)
        if not row["locationid"]:
            raise ValueError("City was not present in the location dimension")

        # The row can then be inserted into the fact table.
        # fact_table.insert(row)

        # By using ensure only new rows will be inserted
        fact_table.ensure(row)

    # Closing all connections.
    connection_wrapper.commit()
    connection_wrapper.close()

    # Do not need to close the connection to bigquery?
    # bigquery_connection.close()
    ext_db_conn.close()


def main():
    # Setting logging level.
    logging.basicConfig(level=logging.INFO)

    # Getting the configuration for Google Cloud Platform.
    gcp_config = get_gcp_config(f"{os.getcwd()}/config/")

    # Setting up BigQuery Client.
    bq_client = init_bq_client(gcp_config["project_id"])
    bq_conn = bq_connector(bq_client)

    # Creating the tables in BigQuery.
    create_tables_hash(bq_client, gcp_config)
    logging.info("Created the BigQuery Tables.")

    # Running the Pygrametl beginner's guide using BigQuery.
    pygrametl_tut(gcp_configuration=gcp_config, bigquery_connection=bq_conn)
    logging.info("Filled all tables!")


main()

# For future implementation, try to use an unique identifier, because now if you rerun the script the same id's get inserted
