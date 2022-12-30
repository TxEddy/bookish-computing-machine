import os
import tomllib

import psycopg2
from pygrametl.datasources import CSVSource, SQLSource
from pygrametl.tables import CachedDimension, FactTable

import pygrametl


# get configuration file.
def get_toml_file(config_path):
    with open(f"{config_path}config.toml", mode="rb") as line:
        config = tomllib.load(line)

    return config


def create_psql_conn(database_name, user, password="", host="", port="5432"):
    psql_conn = psycopg2.connect(
        database=database_name, user=user, host=host, port=port
    )

    return psql_conn


def qry_statement(db_conn, query):
    sql_result = SQLSource(connection=db_conn, query=query)

    return sql_result


def define_dim_table(name_dim_table, name_pk, attr_list, lookupatts=""):

    # if lookupatts is empty return a CachedDimension without lookupatts.
    # else create a CachedDimesnion with lookupatts.

    return CachedDimension(name=name_dim_table, key=name_pk, attributes=attr_list)


def define_fact_table(name_fact_table, name_pk, fk_list, measure_list):

    # if measure_list is empty return a FactTable without measures.
    # else create a FactTable with measures.

    return FactTable(
        name="facttable", key=name_pk, keyrefs=fk_list, measures=measure_list
    )


# Python function needed to split the date into its three parts
def split_date(row):
    """Splits a date represented by a datetime into its three parts"""

    # Splitting of the date into parts
    date = row["date"]
    row["year"] = date.year
    row["month"] = date.month
    row["day"] = date.day


def main():
    # Setting of the configuration files containing the login info.
    # print(f"{os.getcwd()}/config/")
    toml_config = get_toml_file(f"{os.getcwd()}/config/")
    dw_db_info = toml_config["postgresql"]["pygrametl"]
    ext_db_info = toml_config["postgresql"]["eddylim"]

    cwd = os.getcwd()

    # Openening the connections to the different databases.
    dwh_conn = create_psql_conn(
        database_name=dw_db_info["dbname"],
        user=dw_db_info["user"],
        host=dw_db_info["host"],
    )

    ext_db_conn = create_psql_conn(
        database_name=ext_db_info["dbname"],
        user=ext_db_info["user"],
        host=ext_db_info["host"],
    )

    # Creation of connection wrapper.
    connection_wrapper = pygrametl.ConnectionWrapper(connection=dwh_conn)

    # Defining the Dimension and Fact tables.
    dim_book = CachedDimension(name="book", key="bookid", attributes=["book", "genre"])

    dim_time = CachedDimension(
        name="time", key="timeid", attributes=["day", "month", "year"]
    )

    dim_location = CachedDimension(
        name="location",
        key="locationid",
        attributes=["city", "region"],
        lookupatts=["city"],
    )

    fact_table = FactTable(
        name="facttable", keyrefs=["bookid", "locationid", "timeid"], measures=["sale"]
    )

    # Fill the Dimension Location with all the lines from 'region.csv' file.
    # with open(
    #    f"{cwd}/pygrametl/beginner_guide_data/region.csv", "r", 16384
    # ) as csv_region_file:
    #    csv_region_parsed = CSVSource(f=csv_region_file, delimiter=",")

    #    [dim_location.insert(row) for row in csv_region_parsed]

    result_columns = "book", "genre", "city", "date", "sale"

    query = "select book, genre, store, date, sale from sale"
    external_source = SQLSource(
        connection=ext_db_conn, query=query, names=result_columns
    )

    for row in external_source:
        split_date(row)
        # print(row)

        # Lookup the given row. If that fails, insert it.
        # If found, see if values for attributes in otherrefs or
        #  measures have changed and update the found row if necessary (note that values for attributes in keyrefs are not allowed to change).
        # docs: https://chrthomsen.github.io/pygrametl/doc/api/tables.html#pygrametl.tables.AccumulatingSnapshotFactTable.ensure

        # add 'bookid' and 'timeid'
        # So checks if the row exists in both dim_book and dim_time, if it doesn't exist create new id when it does exist use existing id.
        row["bookid"] = dim_book.ensure(row)
        row["timeid"] = dim_time.ensure(row)

        # print(f"Book ID: {row['bookid']}")
        # print(f"Time ID: {row['timeid']}")
        # print(row, "\n")

        # The location dimension is pre-filled, so a missing row is an error.
        row["locationid"] = dim_location.lookup(row)
        if not row["locationid"]:
            raise ValueError("City was not present in the location dimension")

        # The row can then be inserted into the fact table.
        fact_table.insert(row)

    # TO change for BQ, inside the for-loop should be the result of the query against bq. (loop over the rows from the BQ query result), so the ensure should be the same.
    # Only take a look at how the insert and commit should be adapted.
    # The SQLSource is just to query against the database, so this should be a connection to BQ and use this connection to query against the database.
    # TODO: upload the sale table and query against this table.

    connection_wrapper.commit()
    connection_wrapper.close()

    dwh_conn.close()
    ext_db_conn.close()


main()
