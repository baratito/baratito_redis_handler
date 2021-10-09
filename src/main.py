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
    name = product["nombre"]
    presentation = product["presentacion"]
    brand = product["marca"]
    external_id = product["id"]
    data = (name, presentation, brand, external_id)
    query = "INSERT INTO product(name, presentation, brand, max_price, min_price, external_id) VALUES (%s, %s, %s, 0, 0, %s);"
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
    establishment = json.loads(message.get("data").decode("UTF-8"))
    external_id = establishment["id"]
    establishment_type = establishment["sucursalTipo"]
    address = establishment["direccion"]
    county = establishment["localidad"]
    brand = establishment["banderaDescripcion"]
    latitude = establishment["lat"]
    longitude = establishment["lng"]
    name = establishment["sucursalNombre"]

    location = f"POINT({longitude} {latitude})"
    data = (
        name,
        establishment_type,
        address,
        county,
        latitude,
        longitude,
        brand,
        external_id,
        location,
    )
    query = "INSERT INTO establishment(name, establishment_type, address, county, latitude, longitude, brand, external_id, location) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
    conn = get_db_session()
    cursor = conn.cursor()
    try:
        cursor.execute(query, data)
    except Exception as err:
        logger.error(f"cursor.execute() ERROR: {err}")
        conn.rollback()

    conn.commit()
    conn.close()


def category_handler(message):
    logger.info(f"category {message}")
    product_category = json.loads(message.get("data").decode("UTF-8"))

    product_external_id = product_category["id"]
    category_ids = product_category["category"]
    category_id = category_ids.split("-")[0]
    data = (product_external_id, category_id)
    query = "INSERT INTO category_product(product_id, category_id) VALUES ((SELECT id FROM product WHERE external_id=%s), (SELECT ID FROM category where external_id=%s));"
    conn = get_db_session()
    cursor = conn.cursor()
    try:
        cursor.execute(query, data)
    except Exception as err:
        logger.error(f"cursor.execute() ERROR: {err}")
        conn.rollback()

    conn.commit()
    conn.close()


subscriptions = {
    "product": product_handler,
    "site": site_handler,
    "category": category_handler,
}

logger.info("starting handlers")

p.psubscribe(**subscriptions)
thread = p.run_in_thread(sleep_time=0.001)
