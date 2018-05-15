import logging

import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)
import requests

import config

PAGE_SIZE = 1000

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

def omaha_scraper(c):
    l = logging.getLogger("omaha_scraper")
    l.setLevel(logging.INFO)
    l.info('Starting omaha_scraper')

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
    sewer_wgs84 geometry(MULTILINESTRING, 4326),
    upstream_manhole TEXT, -- these are mostly integers but have letters at the end
    downstream_manhole TEXT,
    downstream INTEGER)
""")
    c.execute("CREATE INDEX ON omaha_sewers (upstream_manhole)")
    c.execute("CREATE INDEX ON omaha_sewers (downstream_manhole)")
    # We do searches on the douglas county WKID as its units will be in feet.
    # sewer_wgs84 is just an optimization to cache the transformed geometry.
    c.execute("CREATE INDEX ON omaha_sewers USING GIST (sewer)")

    offset = 0
    feature_cnt = 0
    line_types = {}
    SEWER_QUERY = "https://gis.dogis.org/arcgis/rest/services/Public_Works/Sewer_Network/MapServer/1/query?where=SWR_TYPE%21%3D1+and+LINE_TYPE%21%3D%27Abandoned%27&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=OBJECTID%2CSWR_TYPE%2CLINE_TYPE%2CUP_MANHOLE%2CDN_MANHOLE&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&returnIdsOnly=false&returnCountOnly=false&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&returnDistinctValues=false&resultOffset={0}&resultRecordCount={1}&queryByDistance=&returnExtentsOnly=false&datumTransformation=&parameterValues=&rangeValues=&f=pjson"

    while True:
        l.info("Fetching features from offset %s", offset)
        r = requests.get(SEWER_QUERY.format(offset, SEWER_QUERY)).json()

        fetch_feature_cnt = len(r["features"])
        l.info("Fetched %s features", fetch_feature_cnt)
        feature_cnt += fetch_feature_cnt

        if fetch_feature_cnt == 0:
            l.info("Done fetching object IDs, total count %s", feature_cnt)
            break

        for sewer in r["features"]:
            line_type = sewer['attributes']['LINE_TYPE']
            if line_type not in line_types:
                l.info("Caching omaha_sewer_type %s", line_type)
                c.execute('INSERT INTO omaha_sewer_types (sewer_type) VALUES (%s) RETURNING id', (line_type,))
                line_types[line_type] = c.fetchall()[0][0]
            line_type_id = line_types[line_type]

            line_parts = []
            for line in sewer['geometry']['paths']:
                line_parts.append('(' + ','.join([str(x[0]) + ' ' + str(x[1]) for x in line]) + ')')
            multilinestring = 'MULTILINESTRING({0})'.format(''.join(line_parts))

            c.execute("""
INSERT INTO omaha_sewers
(objectid, sewer_type, sewer, sewer_wgs84, upstream_manhole, downstream_manhole)
VALUES
(%s, %s, ST_GeomFromText(%s, 102704), ST_Transform(ST_GeomFromText(%s, 102704), 4326), %s, %s)
""", (sewer['attributes']['OBJECTID'], line_type_id, multilinestring, multilinestring, sewer['attributes']['UP_MANHOLE'], sewer['attributes']['DN_MANHOLE']))
        offset += PAGE_SIZE
    l.info('Omaha scraper completed.')

def omaha_calc(c):
    l = logging.getLogger('omaha_calc')
    l.setLevel(logging.INFO)
    l.info('Starting omaha_calc')

    c.execute("DROP TABLE IF EXISTS omaha_terminals")
    c.execute("""
CREATE TABLE omaha_terminals(
    terminal text
)""")

    c.execute('SELECT objectid FROM omaha_sewers')
    [omaha_calc_sewer(l, c, objectid[0]) for objectid in c.fetchall()]

    l.info('omaha_calc finished')

def omaha_calc_sewer(l, c, objectid):
    # We have this hacky stuff because we need to do tiny distance searches on the last segment of every sewer.
    # Luckily for us the most downstream segment is also the last one in the geometry.

    c.execute("""
SELECT objectid
FROM omaha_sewers
WHERE upstream_manhole =
    (SELECT downstream_manhole FROM omaha_sewers WHERE objectid = %s)
""", (objectid,))
    downstream_query = c.fetchall()
    if len(downstream_query) == 1:
        c.execute("UPDATE omaha_sewers SET downstream = %s WHERE objectid = %s", (downstream_query[0][0], objectid))
    elif len(downstream_query) > 1:
        #l.warning('FK anomaly on objectid {0}'.format(objectid))
        pass
    else:
        # Chasing foreign keys has failed us, do a distance search.
        # Most queries wind up hitting this condition a few times

        # Get the last point in the head line segment
        c.execute("""
SELECT ST_AsText((dp).geom) AS wkt, ST_AsGeoJSON(ST_Transform((dp).geom, 4326)) as geojson
FROM (SELECT ST_DumpPoints(sewer) as dp FROM omaha_sewers WHERE objectid = %s) inn
""", (objectid,))
        last_point, last_point_geojson = c.fetchall()[-1]

        # This query narrows us down to the 100 nearest objects measured by center point,
        # then uses the geometry-aware ST_Distance on that set to find the closest edge
        # Note that ST_Distance returns units of the SRID, for 102704 that's feet
        c.execute("""
SELECT objectid
FROM (
WITH candidates AS (
   SELECT objectid, sewer
   FROM omaha_sewers
   WHERE objectid != %s
   ORDER BY sewer <-> ST_PointFromText(%s, 102704)
   LIMIT 100
)
SELECT objectid,
   ST_Distance(sewer, ST_PointFromText(%s, 102704)) as dist
FROM candidates
) inn
WHERE dist < 10
ORDER BY dist
LIMIT 1
""", (objectid, last_point, last_point))
        downstream_geom_query = c.fetchall()
        if len(downstream_geom_query) == 0:
            #l.info('No downstreams for objectid {0}, adding to terminals table'.format(objectid))
            c.execute("INSERT INTO omaha_terminals (terminal) VALUES (%s)", (last_point_geojson,))
        elif len(downstream_geom_query) == 1:
            c.execute("UPDATE omaha_sewers SET downstream = %s WHERE objectid = %s", (downstream_geom_query[0][0], objectid))
        else:
            raise Exception('Should never happen')

def main():
    conn = psycopg2.connect(dbname=config.PG_DBNAME, user=config.PG_USER, password=config.PG_PASSWORD, host=config.PG_HOST, port=config.PG_PORT)
    c = conn.cursor()

    omaha_scraper(c)
    omaha_calc(c)

    conn.commit()

    conn.autocommit = True
    c.execute("VACUUM ANALYSE")

    c.close()
    conn.close()

if __name__ == "__main__":
    main()
