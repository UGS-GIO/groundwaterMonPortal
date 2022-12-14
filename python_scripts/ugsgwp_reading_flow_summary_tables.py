import psycopg2

#Database connection information
user="sde"
password=""
host="postgres.geology.utah.gov"
port="5432"
database="ugsgwp"

#Connect to the database
conn = psycopg2.connect(dbname=database,user=user,password=password,host=host)

#Create a cursor
cur=conn.cursor()

#Drop some tables if they exist, then create new ones

cur.execute("DROP TABLE IF EXISTS readings_day_avg;")

sql = '''CREATE TABLE readings_day_avg AS (SELECT date_trunc('day'::text, reading.readingdate) AS readingdate,
    (row_number() OVER ())::integer AS objectid,
    reading.locationid,
    round(avg(reading.measuredlevel), 3) AS measuredlevel,
    round(avg(reading.temperature), 3) AS temperature,
    round(avg(reading.baroefficiencylevel), 3) AS baroefficiencylevel,
    round(avg(reading.measureddtw), 3) AS measureddtw,
    round(avg(reading.driftcorrection), 3) AS driftcorrection,
    round(avg(reading.waterelevation), 3) AS waterelevation,
    round(avg(reading.conductivity), 3) AS conductivity
    FROM reading
  WHERE (1 = 1)
  GROUP BY (date_trunc('day'::text, reading.readingdate)), reading.locationid)
  ORDER BY readingdate;
'''
cur.execute("DROP TABLE IF EXISTS readings_month_avg;")

sql2 = '''CREATE TABLE readings_month_avg AS (SELECT date_trunc('month'::text, reading.readingdate) AS readingdate,
    (row_number() OVER ())::integer AS objectid,
    reading.locationid,
    round(avg(reading.measuredlevel), 3) AS measuredlevel,
    round(avg(reading.temperature), 3) AS temperature,
    round(avg(reading.baroefficiencylevel), 3) AS baroefficiencylevel,
    round(avg(reading.measureddtw), 3) AS measureddtw,
    round(avg(reading.driftcorrection), 3) AS driftcorrection,
    round(avg(reading.waterelevation), 3) AS waterelevation,
    round(avg(reading.conductivity), 3) AS conductivity
    FROM reading
  WHERE (1 = 1)
  GROUP BY (date_trunc('month'::text, reading.readingdate)), reading.locationid)
  ORDER BY readingdate;
'''


cur.execute("DROP TABLE IF EXISTS gw_flow_day_avg;")

sql3 = '''CREATE TABLE gw_flow_day_avg AS (SELECT date_trunc('day'::text, ugs_gw_flow.flowdate) AS flowdate,
    (row_number() OVER ())::integer AS objectid,
    ugs_gw_flow.locationid,
    round(avg( ugs_gw_flow.discharge), 3) AS discharge
    FROM ugs_gw_flow
  WHERE (1 = 1)
  GROUP BY (date_trunc('day'::text, ugs_gw_flow.flowdate)),  ugs_gw_flow.locationid)
  ORDER BY flowdate;
'''

cur.execute("DROP TABLE IF EXISTS gw_flow_month_avg;")

sql4 = '''CREATE TABLE gw_flow_month_avg AS (SELECT date_trunc('month'::text, ugs_gw_flow.flowdate) AS flowdate,
    (row_number() OVER ())::integer AS objectid,
    ugs_gw_flow.locationid,
    round(avg( ugs_gw_flow.discharge), 3) AS discharge
    FROM ugs_gw_flow
  WHERE (1 = 1)
  GROUP BY (date_trunc('month'::text, ugs_gw_flow.flowdate)),  ugs_gw_flow.locationid)
  ORDER BY flowdate;
'''

#grant privileges to table users

sql5 = '''GRANT SELECT ON readings_day_avg TO ugsgwp_reader;
GRANT SELECT ON readings_month_avg TO ugsgwp_reader;
GRANT SELECT ON gw_flow_day_avg TO ugsgwp_reader;
GRANT SELECT ON gw_flow_month_avg TO ugsgwp_reader;
'''

# Execute the sql code
cur.execute(sql);
cur.execute(sql2);
cur.execute(sql3);
cur.execute(sql4);
cur.execute(sql5);


print("Summary tables were created successfully...")


#Commit the code to the database
conn.commit()

#Close the cursor and the database connection
cur.close()
conn.close()

