from functools import wraps

import psycopg2
import psycopg2.extensions
import psycopg2.extras
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

from flask import Flask, render_template, g, jsonify, Response, request, redirect, url_for
app = Flask(__name__)

import config

app.config.update(config.__dict__)

@app.before_request
def before_request():
   g.conn = psycopg2.connect(dbname=config.PG_DBNAME, user=config.PG_USER, password=config.PG_PASSWORD, host=config.PG_HOST, port=config.PG_PORT)
   g.c = g.conn.cursor()

@app.teardown_request
def after_request(response_class):
   g.c.close()
   g.conn.close()

def validate(flask_fn):
   @wraps(flask_fn)
   def wrapped(*args, **kwargs):
      try:
         city = request.args["city"]
      except KeyError:
         return redirect(url_for("map_page", city=config.DEFAULT_CITY))

      if city not in config.CITIES:
         return redirect(url_for("map_page", city=config.DEFAULT_CITY))

      return flask_fn(*args, **kwargs)
   return wrapped

@app.route('/track_poop')
@validate
def track_poop():
   if 'lat' not in request.args or 'lng' not in request.args:
      return

   try:
      lat = float(request.args["lat"])
      lon = float(request.args["lng"])
   except ValueError:
      return

   city_conf = config.CITIES[request.args["city"]]

   if not ((city_conf["lat_lt"] <= lat) and (city_conf["lat_gt"] >= lat)) \
      or \
      not ((city_conf["lon_lt"] <= lon) and (city_conf["lon_gt"] >= lon)):
      return

   g.c.execute("""
SELECT objectid
FROM (
WITH candidates AS (
   SELECT objectid, sewer
   FROM omaha_sewers
   ORDER BY sewer <-> ST_Transform('SRID=4326;POINT(%s %s)'::geometry, 102704)
   LIMIT 100
)
SELECT objectid,
   ST_Distance(sewer, ST_Transform('SRID=4326;POINT(%s %s)'::geometry, 102704)) as dist
FROM candidates
) inn
ORDER BY dist
LIMIT 1
""", (lon, lat, lon, lat))
   candidates = g.c.fetchall()
   if len(candidates) == 0:
      return
   candidates = candidates[0]

   # We have this hacky stuff because we need to do tiny distance searches on the last segment of every sewer.
   # Luckily for us the most downstream segment is also the last one in the geometry.

   # The geoms that we want at the end
   sewers = set(candidates)
   # The current sewer is the head for its children.
   heads = set(candidates)

   while True:
      # This will hold the children of heads.
      # Eventually we run out of children and it'll be empty. That is when we break the loop.
      downstreams = set()
      for head in heads:
         g.c.execute("""
SELECT objectid
FROM omaha_sewers
WHERE upstream_manhole =
    (SELECT downstream_manhole FROM omaha_sewers WHERE objectid = %s)
""", (head,))
         downstream_query = g.c.fetchall()
         for x in downstream_query:
            x = x[0]
            sewers.add(x)
            downstreams.add(x)

         # Chasing foreign keys has failed us, do a distance search.
         # Note that ST_Distance returns units of the SRID, for 102704 that's feet
         # Most queries wind up hitting this condition a few times
         if len(downstream_query) == 0:
            # Get the last point in the head line segment
            g.c.execute("""
SELECT ST_AsText((dp).geom) AS wkt
FROM (SELECT ST_DumpPoints(sewer) as dp FROM omaha_sewers WHERE objectid = %s) inn
""", (head,))
            last_point = g.c.fetchall()[-1][0]

            g.c.execute("""
SELECT objectid
FROM (
WITH candidates AS (
   SELECT objectid, sewer
   FROM omaha_sewers
   WHERE objectid NOT IN %s
   ORDER BY sewer <-> ST_PointFromText(%s, 102704)
   LIMIT 100
)
SELECT objectid,
   ST_Distance(sewer, ST_PointFromText(%s, 102704)) as dist
FROM candidates
) inn
WHERE dist < 5
ORDER BY dist
LIMIT 1
""", (tuple(sewers), last_point, last_point))
            downstream_query = g.c.fetchall()
            for x in downstream_query:
               x = x[0]
               sewers.add(x)
               downstreams.add(x)

            # we are at the very end of the system, give up
            break
      if len(downstreams) == 0:
         break
      heads = downstreams

   g.c.execute('SELECT ST_AsGeoJSON(ST_Transform(sewer, 4326)) FROM omaha_sewers where objectid in ({0})'\
      .format(','.join([str(x) for x in sewers])))
   sewer_json = '[' + ','.join((x[0] for x in g.c if x[0] != None)) + ']'
   return sewer_json

@app.route('/')
@validate
def map_page():
   return render_template('map.html')

if __name__ == '__main__':
   app.run(host='0.0.0.0', debug=True)
