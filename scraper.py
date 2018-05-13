import logging

import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
import requests

import config

PAGE_SIZE = 1000

logging.basicConfig()

def omaha(c):
    l = logging.getLogger("omaha")

    # the douglas county SRID 102704
    c.execute("SELECT 1 FROM spatial_ref_sys WHERE srid = 102704")
    if len(c.fetchall()) == 0:
        c.execute("""
INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text) values ( 102704, 'ESRI', 102704, '+proj=lcc +lat_1=40 +lat_2=43 +lat_0=39.83333333333334 +lon_0=-100 +x_0=500000.0000000002 +y_0=0 +datum=NAD83 +units=us-ft +no_defs ')
""")

    c.execute("DROP TABLE IF EXISTS omaha_sewers")
    c.execute("DROP TABLE IF EXISTS omaha_sewer_types")

    c.execute("CREATE TABLE omaha_sewer_types (id SERIAL PRIMARY KEY, sewer_type TEXT)")
    c.execute("""
CREATE TABLE omaha_sewers (
    objectid INTEGER PRIMARY KEY,
    sewer_type INTEGER REFERENCES omaha_sewer_types(id),
    sewer geometry(MULTILINESTRING, 102704),
    upstream_manhole TEXT, -- these are mostly integers but have letters at the end
    downstream_manhole TEXT)
""")
    c.execute("CREATE INDEX ON omaha_sewers (upstream_manhole)")
    c.execute("CREATE INDEX ON omaha_sewers (downstream_manhole)")
    c.execute("CREATE INDEX ON omaha_sewers USING GIST (sewer)")

    offset = 0
    feature_cnt = 0
    line_types = {}
    SEWER_QUERY = "https://gis.dogis.org/arcgis/rest/services/Public_Works/Sewer_Network/MapServer/1/query?where=SWR_TYPE%21%3D1+and+LINE_TYPE%21%3D%27Abandoned%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2CSWR_TYPE%2CLINE_TYPE%2CUP_MANHOLE%2CDN_MANHOLE&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset={0}&resultRecordCount={1}&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=pjson"

    while True:
        l.warning("Fetching features from offset %s", offset)
        r = requests.get(SEWER_QUERY.format(offset, SEWER_QUERY)).json()

        fetch_feature_cnt = len(r["features"])
        l.warning("Fetched %s features", fetch_feature_cnt)
        feature_cnt += fetch_feature_cnt

        if fetch_feature_cnt == 0:
            l.warning("Done fetching object IDs, total count %s", feature_cnt)
            break

        for sewer in r["features"]:
            line_type = sewer['attributes']['LINE_TYPE']
            if line_type not in line_types:
                l.warning("Caching omaha_sewer_type %s", line_type)
                c.execute('INSERT INTO omaha_sewer_types (sewer_type) VALUES (%s) RETURNING id', (line_type,))
                line_types[line_type] = c.fetchall()[0][0]
            line_type_id = line_types[line_type]

            line_parts = []
            for line in sewer['geometry']['paths']:
                line_parts.append('(' + ','.join([str(x[0]) + ' ' + str(x[1]) for x in line]) + ')')
            multilinestring = 'MULTILINESTRING({0})'.format(''.join(line_parts))

            c.execute("""
INSERT INTO omaha_sewers
(objectid, sewer_type, sewer, upstream_manhole, downstream_manhole)
VALUES
(%s, %s, ST_GeomFromText(%s, 102704), %s, %s)
""", (sewer['attributes']['OBJECTID'], line_type_id, multilinestring, sewer['attributes']['UP_MANHOLE'], sewer['attributes']['DN_MANHOLE']))
        offset += PAGE_SIZE

def main():
    conn = psycopg2.connect(dbname=config.PG_DBNAME, user=config.PG_USER, password=config.PG_PASSWORD, host=config.PG_HOST, port=config.PG_PORT)
    c = conn.cursor()

    omaha(c)

    conn.commit()

    conn.autocommit = True
    c.execute("VACUUM ANALYSE")

    c.close()
    conn.close()

if __name__ == "__main__":
    main()
