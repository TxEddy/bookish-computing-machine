import tomllib

from google.cloud import bigquery
from google.cloud.bigquery import client

# client = bigquery.Client(project='testenvironment-338811')

# query_job = client.query("select * from test.woonplaats limit 10")

# results = query_job.result()

# for row in results:
#     print(row)

# get configuration file.
def get_toml_file(config_path):
    with open(f"{config_path}config.toml", mode="rb") as line:
        config = tomllib.load(line)

    return config


# get configuration file.
def get_gcp_config(config_path):
    with open(f"{config_path}config.toml", mode="rb") as line:
        config = tomllib.load(line)

    return config["gcp"]["vintus"]


def init_bq_client(project_id):
    return bigquery.Client(project=project_id)


def create_bq_table(name_table, schema):
    return bigquery.Table(name_table, schema=schema)


def split_date(row):
    """Splits a date represented by a datetime into its three parts"""

    # Splitting of the date into parts
    date = row["date"]
    row["year"] = date.year
    row["month"] = date.month
    row["day"] = date.day


def create_tables(client, toml_config):
    book_schema = [
        bigquery.SchemaField("bookid", "integer", mode="required"),
        # bigquery.SchemaField("book", "string", mode="nullable"),
        bigquery.SchemaField("title", "string", mode="nullable"),
        bigquery.SchemaField("genre", "string", mode="nullable"),
    ]

    time_schema = [
        bigquery.SchemaField("timeid", "integer", mode="required"),
        bigquery.SchemaField("day", "integer", mode="nullable"),
        bigquery.SchemaField("month", "integer", mode="nullable"),
        bigquery.SchemaField("year", "integer", mode="nullable"),
    ]

    location_schema = [
        bigquery.SchemaField("locationid", "integer", mode="required"),
        bigquery.SchemaField("city", "string", mode="nullable"),
        bigquery.SchemaField("region", "string", mode="nullable"),
    ]

    facttable_schema = [
        bigquery.SchemaField("bookid", "integer", mode="required"),
        bigquery.SchemaField("locationid", "integer", mode="required"),
        bigquery.SchemaField("timeid", "integer", mode="required"),
        bigquery.SchemaField("sale", "integer", mode="required"),
    ]

    dim_book = create_bq_table(
        f"{toml_config['project_id']}.pygrametl.book", book_schema
    )

    dim_time = create_bq_table(
        f"{toml_config['project_id']}.pygrametl.time", time_schema
    )

    dim_location = create_bq_table(
        f"{toml_config['project_id']}.pygrametl.location", location_schema
    )

    fact_table = create_bq_table(
        f"{toml_config['project_id']}.pygrametl.facttable", facttable_schema
    )

    client.create_table(dim_book)
    client.create_table(dim_time)
    client.create_table(dim_location)
    client.create_table(fact_table)


def create_tables_hash(client, toml_config):
    book_schema = [
        bigquery.SchemaField("bookid", "integer", mode="required"),
        bigquery.SchemaField("title", "string", mode="nullable"),
        bigquery.SchemaField("genre", "string", mode="nullable"),
        bigquery.SchemaField(
            "book_hash_id",
            "string",
            mode="required",
            # default_value_expression="GENERATE_UUID()",
        ),
    ]

    time_schema = [
        bigquery.SchemaField("timeid", "integer", mode="required"),
        bigquery.SchemaField("day", "integer", mode="nullable"),
        bigquery.SchemaField("month", "integer", mode="nullable"),
        bigquery.SchemaField("year", "integer", mode="nullable"),
        bigquery.SchemaField("time_hash_id", "string", mode="required"),
    ]

    location_schema = [
        bigquery.SchemaField("locationid", "integer", mode="required"),
        bigquery.SchemaField("city", "string", mode="nullable"),
        bigquery.SchemaField("region", "string", mode="nullable"),
    ]

    facttable_schema = [
        bigquery.SchemaField("book_hash_id", "string", mode="required"),
        bigquery.SchemaField("locationid", "integer", mode="required"),
        bigquery.SchemaField("time_hash_id", "string", mode="required"),
        bigquery.SchemaField("sale", "integer", mode="required"),
    ]

    dim_book = create_bq_table(
        f"{toml_config['project_id']}.pygrametl_hash.book", book_schema
    )

    dim_time = create_bq_table(
        f"{toml_config['project_id']}.pygrametl_hash.time", time_schema
    )

    dim_location = create_bq_table(
        f"{toml_config['project_id']}.pygrametl_hash.location", location_schema
    )

    fact_table = create_bq_table(
        f"{toml_config['project_id']}.pygrametl_hash.facttable", facttable_schema
    )

    client.create_table(dim_book)
    client.create_table(dim_time)
    client.create_table(dim_location)
    client.create_table(fact_table)
