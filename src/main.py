import redis
from psycopg2 import connect, sql
from structlog import get_logger

from settings import (
    DB_HOST,
    DB_NAME,
    DB_PASS,
    DB_PORT,
    DB_USER,
    REDIS_HOST,
    REDIS_PASSWORD,
    REDIS_PORT,
)

logger = get_logger()

logger.info(f"starting redis connection {REDIS_HOST}:{REDIS_PORT}")

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, password=REDIS_PASSWORD)
p = r.pubsub()


def get_db_session():
    try:
        conn = connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, password=DB_PASS, port=DB_PORT)
        return conn
    except Exception as err:
        logger.error(f"\npsycopg2 error:{err}")


import json


def product_handler(message):
    logger.info(f"processing product {message}")
    product = json.loads(message.get("data").decode("UTF-8"))
    id = product["id"]
    name = product["nombre"]
    presentation = product["presentacion"]
    brand = product["marca"]
    data = (id, name, presentation, brand)
    query = "INSERT INTO product(id, name, presentation, brand, max_price, min_price) VALUES (%s, %s, %s, %s, 0, 0);"
    conn = get_db_session()
    cursor = conn.cursor()
    try:
        cursor.execute(query, data)

    except Exception as err:
        logger.error(f"cursor.execute() ERROR: {err}")
        conn.rollback()

    conn.commit()
    conn.close()


def site_handler(message):
    logger.info(f"site {message}")


subscriptions = {
    "product": product_handler,
    "site": site_handler,
}

logger.info("starting handlers")

p.psubscribe(**subscriptions)
thread = p.run_in_thread(sleep_time=0.001)
