import logging

import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

import config

logging.basicConfig(format='%(asctime)s %(name)s:%(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import omaha

def main():
    conn = psycopg2.connect(dbname=config.PG_DBNAME, user=config.PG_USER, password=config.PG_PASSWORD, host=config.PG_HOST, port=config.PG_PORT)
    c = conn.cursor()

    omaha.omaha(c)

    conn.commit()

    conn.autocommit = True
    c.execute("VACUUM ANALYSE")

    c.close()
    conn.close()

if __name__ == "__main__":
    main()
