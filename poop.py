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
   parent = g.c.fetchall()
   if len(parent) == 0:
      return
   parent = parent[0]

   g.c.execute("""
SELECT ST_AsGeoJSON(sewer_wgs84) FROM omaha_sewers where objectid in (
    WITH RECURSIVE sewers(objectid, downstream, all_children) as (
       SELECT objectid, downstream, array[objectid] as all_children
       FROM omaha_sewers
       WHERE objectid = %s

       UNION ALL

       SELECT dst.objectid, dst.downstream, sewers.all_children || dst.objectid as all_children
       FROM omaha_sewers dst
       JOIN sewers ON sewers.downstream = dst.objectid
           AND dst.objectid <> ALL(sewers.all_children)
    )
    SELECT objectid FROM sewers
)
""", (parent,))

   sewer_json = u'[' + u','.join((x[0] for x in g.c)) + u']'
   return sewer_json

@app.route('/')
@validate
def map_page():
   query = "SELECT terminal FROM {0}_terminals".format(request.args["city"])
   g.c.execute(query)
   terminals = u'[' + u','.join((x[0] for x in g.c)) + u']'

   return render_template('map.html', terminals=terminals)

if __name__ == '__main__':
   app.run(host='0.0.0.0', debug=True)
