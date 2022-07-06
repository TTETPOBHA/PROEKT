#добавление библиотек
import sqlite3
import csv
import pandas as pd
from geopy.distance import geodesic as GD
import  numpy
from pyasn1_modules.rfc2315 import data
from sqlalchemy import create_engine
import pandas as df




conn = sqlite3.connect('ostanovki.db')

#создание бд
cursor = conn.cursor()
def ostanovki_db():

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS station(
        bus_stop TEXT,
        distance INT,
        База_ЖК TEXT);
    """)
    conn.commit()

ostanovki_db()


#def house(bus_stop,База_ЖК):
 #   cursor.execute("""
  #      INSERT INTO station(bus_stop,База_ЖК)
   #     VALUES (?, ?, ? )
    #""",[bus_stop,База_ЖК])
    #conn.commit()

def Itogi_db():

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS itogi(
        bus_stop TEXT,
        distance INT,
        База_ЖК TEXT);
    """)
    conn.commit()

Itogi_db()

def sortirovka(bus_stop,distance,База_ЖК):
    cursor.execute("""
        INSERT INTO itogi (bus_stop,distance,База_ЖК)
        VALUES (?, ?, ? )
    
    """,[bus_stop,distance,База_ЖК])
    conn.commit()


engine = create_engine(r'sqlite:///C:\Users\lmyro\PycharmProjects\PROEK\ostanovki.db', echo=False)


database_bus_stops = []
database_homes = []

massiv = {}

stops_df1 = pd.read_csv(r'ostanovki.csv', encoding='Windows-1251', sep=';')
with engine.begin() as connection:
    stops_df1.to_sql('station', con=connection, if_exists='replace')

stops_df2 = pd.read_csv(r'baza_hc.csv', sep=',')
with engine.begin() as connection:
    stops_df2.to_sql('HC', con=connection, if_exists='replace')
#stops_df1 = pd.read_csv(r'C:\Users\lmyro\PycharmProjects\PROEK\ostanovki.csv', encoding='Windows-1251', sep=';')
##ith engine.begin() as connection:
 #   stops_df1.to_sql('station', con=connection, if_exists='replace')

#stops_df2 = pd.read_csv(r'C:\Users\lmyro\PycharmProjects\PROEK\baza_hc.csv', sep=',')
#with engine.begin() as connection:
 #   stops_df2.to_sql('station', con=connection, if_exists='replace')
#обработка

for row_idx, row in stops_df2.iterrows():
    home_name = row["База_ЖК"]
    coordinates_home = row['Координаты центра']

    pervaya_scobca = coordinates_home.index('(')
    vtoraya_scobca = coordinates_home.index(')')
    coord = coordinates_home[pervaya_scobca + 1:vtoraya_scobca]
    coord = coord.replace(' ', ',')

    #print(coord)
    for stop_row_idx, stop_row in stops_df1.iterrows():
        lat_and_lon_bus_stop = f'{stop_row["Longitude_WGS84"]}, {stop_row["Latitude_WGS84"]}'
        if GD(coord, lat_and_lon_bus_stop).m <= 1000:  # в радиусе 1000 м
            sortirovka(home_name,stop_row['StationName'], round(GD(coord, lat_and_lon_bus_stop).m))



