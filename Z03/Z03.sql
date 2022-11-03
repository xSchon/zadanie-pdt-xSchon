---  Author: Martin Schon ---
---  PDT_Z02              ---

----  S02  ----------------------------------------------------------------
SELECT name, ST_Astext(ST_Transform(way, 4326))
FROM planet_osm_polygon
WHERE admin_level = '4';
----  E02  ----------------------------------------------------------------

----  S03  ----------------------------------------------------------------
SELECT name, ST_Area(ST_Transform(way, 4326)::geography) / 1000000 as km2
FROM planet_osm_polygon
WHERE admin_level = '4'
ORDER BY km2;
----  E03  ----------------------------------------------------------------

----  S04  ----------------------------------------------------------------
INSERT INTO planet_osm_polygon
    ("addr:housename", "addr:housenumber", name, "natural", z_order, tags, way)
    VALUES ('Domov', 5, 'Martin Schon', 'grassland', 0, '',
    ST_Transform(ST_SetSRID(ST_GeomFromText('POLYGON((
    17.309516058731177 48.3300009742030,
    17.30967503789495 48.33002440991696,
    17.30964503789495 48.330100328261695,
    17.309725504163416 48.330110328261695,
    17.309705504163416 48.330182479773576,
    17.309471131731286 48.33015394805975,
    17.309516058731177 48.3300009742030))'), 4326), 3857));

UPDATE planet_osm_polygon
SET way_area = ROUND(ST_Area(way))
WHERE "addr:housename" = 'Domov';

SELECT ST_Transform(way, 4326) FROM planet_osm_polygon WHERE "addr:housename" = 'Domov';
----  E04  ------------------------------------------------------------

----  S05  ----------------------------------------------------------------
SELECT kraj.name
FROM planet_osm_polygon AS dom
CROSS JOIN (SELECT way, name
            FROM planet_osm_polygon
            WHERE admin_level = '4') AS kraj
WHERE "addr:housename" = 'Domov'
AND ST_Within(dom.way, kraj.way);
----  E05  ----------------------------------------------------------------

----  S06  ----------------------------------------------------------------
SELECT name, way, ST_GeometryType(way), ST_Srid(way)
FROM planet_osm_point;

INSERT INTO planet_osm_point
    (name, way)
VALUES ('Martin Schon',
        ST_Transform(ST_SetSRID(
  ST_GeomFromText('POINT(17.309563464945274 48.33010486703582)'),
        4326), 3857));

SELECT ST_Transform(way, 4326), name
FROM planet_osm_point WHERE name = 'Martin Schon';
----  E06  ----------------------------------------------------------------

----  S07  ----------------------------------------------------------------
SELECT ST_Transform(home.way, 4326) AS home,
       ST_Transform(me.way, 4326) AS me,
       ST_Within(me.way, home.way)
FROM planet_osm_polygon home
CROSS JOIN (SELECT way
            FROM planet_osm_point
            WHERE name = 'Martin Schon') AS me
WHERE name = 'Martin Schon';
----  E07  ----------------------------------------------------------------

----  S08  ----------------------------------------------------------------
SELECT
    ST_Distance(
        ST_Transform(school.way, 4326)::geography,
        ST_Transform(me.way, 4326)::geography) / 1000
        AS distance_km
FROM planet_osm_polygon school
CROSS JOIN (SELECT way
            FROM planet_osm_point
            WHERE name = 'Martin Schon') AS me
WHERE name = 'Fakulta informatiky a informačných technológií STU';
----  E08  ----------------------------------------------------------------

----  S09  ----------------------------------------------------------------
-- QGIS commands
SELECT name, way
FROM "public"."planet_osm_polygon"
WHERE admin_level = '4';  -- don't forget to delete ; before QGIS

SELECT way
FROM "public"."planet_osm_polygon"
WHERE name = 'Martin Schon';
----  E09  ----------------------------------------------------------------

----  S10  ----------------------------------------------------------------
SELECT name,
   --  ST_AsText(ST_Transform(ST_Centroid(way), 4326)),
       ST_Y(ST_Transform(ST_Centroid(way), 4326)) AS latitude,
       ST_X(ST_Transform(ST_Centroid(way), 4326)) AS longitude,
       ST_SRID((ST_Transform(ST_Centroid(way), 4326))) AS SRID
FROM planet_osm_polygon
WHERE admin_level = '4'
ORDER BY ST_Area(way)
LIMIT 1;
----  E10  ----------------------------------------------------------------

----  S11  ----------------------------------------------------------------
CREATE TABLE roads_close_Pezinok_Malacky
(id serial PRIMARY KEY,
 osm_id BIGINT,
 way geometry(LineString, 3857),
 distance REAL,
 name VARCHAR(255),
 highway VARCHAR(255)
);

INSERT INTO roads_close_Pezinok_Malacky (way, osm_id, distance, name, highway)
SELECT  close_roads.way,
        close_roads.osm_id,
        ST_Distance(ST_Transform(close_roads.way, 4326)::geography, border.border::geography) / 1000 as km,
        close_roads.name,
        close_roads.highway
FROM planet_osm_roads close_roads,
     (SELECT
      ST_LineMerge(ST_Transform(ST_Intersection(malacky.way, pezinok.way), 4326)) as border
      FROM planet_osm_polygon AS malacky,
           (SELECT name, way
           FROM planet_osm_polygon
           WHERE name LIKE 'okres Pezinok') AS pezinok
      WHERE malacky.name LIKE 'okres Malacky') AS border
WHERE close_roads.highway
NOT IN ('proposed', 'footway', 'cycleway', 'platform', 'construction', 'path', 'service')
AND close_roads.highway IS NOT NULL
AND (close_roads.motorcar != 'no' OR close_roads.motorcar IS NULL)
AND ST_Distance(ST_Transform(close_roads.way, 4326)::geography, border.border::geography) / 1000 < 10;

SELECT COUNT(*) FROM roads_close_Pezinok_Malacky;
SELECT st_transform(way, 4326) FROM roads_close_Pezinok_Malacky;
SELECT * FROM roads_close_Pezinok_Malacky;
----  E11  ----------------------------------------------------------------

----  S12  ----------------------------------------------------------------
SELECT cadastre.idn5 AS cadastre_id,
       cadastre.nm5 AS cadastre_name,
       ref AS road_num,
       ST_Length(ST_Intersection(ST_Transform(roads.way, 4326)::geography,
                        ST_Transform(cadastre.way, 4326)::geography)) / 1000 as len_km,
       ST_Transform(ST_Intersection(ST_Transform(roads.way, 4326)::geography,
                    ST_Transform(cadastre.way, 4326)::geography)::geometry, 4326) AS road_visualisation,
       ST_Transform(cadastre.way, 4326) AS cadastre_visualisation
FROM
    planet_osm_roads roads,
    (SELECT idn5, nm5, ST_Transform(shape, 3857) AS way FROM ku_0 WHERE lau1 = 'Pezinok') AS cadastre
WHERE ST_Intersects(roads.way, cadastre.way)
AND roads.highway
NOT IN ('proposed', 'footway', 'cycleway', 'platform', 'construction', 'path', 'service')
AND roads.highway IS NOT NULL
ORDER BY ST_Length(ST_Intersection(roads.way, cadastre.way)) DESC
LIMIT 1;
----  E12  ----------------------------------------------------------------

----  S13  ----------------------------------------------------------------
SELECT ST_Area(ST_Transform(
       ST_CollectionExtract(
       ST_Intersection(slovakia.way,
       ST_Difference(okolie.way, bratislava.way)), 3), 4326
           )::geography) / 1000000 as km2,
       ST_Transform(
       ST_CollectionExtract(
       ST_Intersection(slovakia.way,
       ST_Difference(okolie.way, bratislava.way)), 3), 4326
           ) AS Bratislava_Okolie
FROM
(SELECT ST_Transform(ST_Buffer(ST_Transform(way, 4326)::geography, 20000)::geometry, 3857) AS way
FROM planet_osm_polygon
WHERE name = 'Bratislava' AND admin_level = '6') AS okolie,
(SELECT way
FROM planet_osm_polygon
WHERE name = 'Bratislava' AND admin_level = '6') AS bratislava,
(SELECT way
FROM planet_osm_polygon
WHERE admin_level = '2'
AND name = 'Slovensko') as slovakia;
----  E13  ----------------------------------------------------------------



