"""Fill bronze.trino_query_events and ref.user_teams with fake demo data."""
import random
import uuid
from datetime import datetime, timedelta

SEED = 42
NUM_QUERIES = 2000
DAYS = 30
# Fixed period (not "today") so the example queries always return the same results.
SAMPLE_END = datetime(2026, 10, 25)
SAMPLE_START = SAMPLE_END - timedelta(days=DAYS)  # 2026-09-25

FAR_PAST = datetime(2020, 1, 1)
FAR_FUTURE = datetime(9999, 12, 31)
TEAM_CHANGE = datetime(2026, 10, 10)

# (user_name, team, valid_from, valid_to)
USER_TEAMS = [
    ("anna", "analytics", FAR_PAST, FAR_FUTURE),
    ("boris", "analytics", FAR_PAST, FAR_FUTURE),
    ("chen", "analytics", FAR_PAST, FAR_FUTURE),
    ("svc_tableau", "analytics", FAR_PAST, FAR_FUTURE),  # service account
    ("dmitry", "data-platform", FAR_PAST, FAR_FUTURE),
    ("eva", "data-platform", FAR_PAST, FAR_FUTURE),
    ("svc_airflow", "data-platform", FAR_PAST, FAR_FUTURE),  # service account
    ("fatima", "marketing", FAR_PAST, FAR_FUTURE),
    ("george", "marketing", FAR_PAST, FAR_FUTURE),
    ("hana", "marketing", FAR_PAST, FAR_FUTURE),
    # olga moved from marketing to analytics in the middle of the period
    ("olga", "marketing", FAR_PAST, TEAM_CHANGE),
    ("olga", "analytics", TEAM_CHANGE, FAR_FUTURE),
    ("ivan", "finance", FAR_PAST, FAR_FUTURE),
    ("julia", "finance", FAR_PAST, FAR_FUTURE),
    ("karl", "finance", FAR_PAST, FAR_FUTURE),
]
UNMAPPED_USER = "temp_contractor"  # no row in ref.user_teams -> team UNKNOWN
USERS = sorted({row[0] for row in USER_TEAMS}) + [UNMAPPED_USER]

TABLES = [
    "iceberg.sales.orders",
    "iceberg.sales.order_items",
    "iceberg.sales.customers",
    "iceberg.marketing.campaigns",
    "iceberg.marketing.web_sessions",
    "iceberg.finance.invoices",
    "iceberg.finance.payments",
    "iceberg.product.events",
    "iceberg.product.users",
    "iceberg.platform.job_runs",
]
TABLE_WEIGHTS = [10, 6, 5, 3, 4, 3, 3, 5, 2, 1]  # orders is the most popular


def make_events(rng):
    events = []
    for _ in range(NUM_QUERIES):
        start = SAMPLE_START + timedelta(seconds=rng.randint(0, DAYS * 86400 - 3600))
        end = start + timedelta(seconds=rng.randint(1, 600))
        # choices() may pick the same table twice, like a self-join
        tables = rng.choices(TABLES, weights=TABLE_WEIGHTS, k=rng.randint(1, 3))
        events.append((
            str(uuid.UUID(int=rng.getrandbits(128))),
            rng.choice(USERS),
            "FAILED" if rng.random() < 0.05 else "FINISHED",
            start,
            end,
            rng.randint(100, 3_600_000),                 # up to 1 CPU hour
            rng.randint(10, 8_000) * 1024 * 1024,        # 10 MB .. 8 GB
            rng.randint(1, 50_000) * 1024 * 1024,        # 1 MB .. ~50 GB
            tables,
            end + timedelta(seconds=rng.randint(1, 120)),
        ))
    # ~2% duplicates: same query_id, ingested later
    for event in rng.sample(events, k=NUM_QUERIES // 50):
        events.append(event[:-1] + (event[-1] + timedelta(minutes=rng.randint(1, 60)),))
    return events


def sql_literal(value):
    if isinstance(value, str):
        return f"'{value}'"
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value:%Y-%m-%d %H:%M:%S}'"
    if isinstance(value, list):
        return "array(" + ", ".join(sql_literal(v) for v in value) + ")"
    return str(value)


def insert_overwrite(spark, table, rows):
    # Plain SQL VALUES instead of spark.createDataFrame(): no Python worker
    # processes are needed, which are slow and flaky to start on Windows.
    values = ",\n".join("(" + ", ".join(sql_literal(v) for v in row) + ")" for row in rows)
    spark.sql(f"INSERT OVERWRITE {table} VALUES {values}")


def generate_sample_data(spark):
    rng = random.Random(SEED)
    # Overwrite, so running the demo twice gives the same data
    insert_overwrite(spark, "bronze.trino_query_events", make_events(rng))
    insert_overwrite(spark, "ref.user_teams", USER_TEAMS)
