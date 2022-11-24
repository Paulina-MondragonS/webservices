#!/usr/bin/env python
# coding: utf-8

# In[1]:


#!pip install pymysql
#!pip install mysqlclient
#!pip install MySQL-python
#!pip install python3-mysqldb
#!pip3 install PyMySQL
import pymysql
pymysql.install_as_MySQLdb()
from urllib.request import urlopen
import json
from datetime import datetime
import sys
#!pip install pymysql
#!pip install mysqlclient
#!pip install MySQL-python
#!pip install python3-mysqldb
#!pip3 install PyMySQL
import pymysql
pymysql.install_as_MySQLdb()
from urllib.request import urlopen
import json
import time
import pandas as pd
from pandas import json_normalize
print("empieza a leer el codigo para insertar en enlace")
url = "https://telemetry.apis.enlacefl.com/clients/389/users/39836/assets/current-position?key=AIzaSyClrO9y9EmcJazI4-9RgKiwkAUg_lHt_M8"
response = urlopen(url)
data_json = json.loads(response.read())
df=json_normalize(data_json['data'])
df
df = df.drop(df.columns[[0,7, 8, 9]], axis='columns')
df
#print(df.columns.values)
#type(df.columns.values)
df.columns = [ 'assetid', 'numberPlates', 'vehicleBrand', 'vehicleModel', 'vehicleYear','vin',
              'serialNumber','speed','gpsDistance','driverId', 'driverName','driverLastName',
              'driverKey', 'diagnosticStatusId', 'status', 'operationalStatusId','operationalStatus',
              'events','ignition','orientation','orientationLabel', 'satellites','temperatures', 'nearestCityReference','distance',
              'city','orientationcity','fecha','latitude', 'longitude','street', 'altitude','isSatelliteSource']
df
df = df.drop(df.columns[[17,22]], axis='columns')
df
df2=df["fecha"].str.split('[T+]', expand=True)
df2
df2 = df2.drop(df2.columns[[2]], axis='columns')
df2
df2["fecha"] = df2[0].map(str) + " " + df2[1]
df2
df2 = df2.drop(df2.columns[[0,1]], axis='columns')
df2
df2['fecha'] = pd.to_datetime(df2['fecha'])
df2
df2['fecha'] = pd.to_datetime(df['fecha'],format="%Y-%m-%d")
df2
df2['fecha'] = df2['fecha'].dt.tz_convert('America/Mexico_City')
df2
df2['dates'] = pd.to_datetime(df2['fecha']).dt.date
df2['times'] = pd.to_datetime(df2['fecha']).dt.time
df2
df2 = df2.drop(df2.columns[[0]], axis='columns')
df2
df5= df2['dates'].astype(str)
df5
df6= df2['times'].astype(str)
df6
df7=pd.concat([df5, df6], axis=1)
df7
df7["time"] = df7["dates"] +' '+df7["times"]
df7
df7 = df7.drop(df7.columns[[0,1]], axis='columns')
df7
enlace = pd.concat([df, df7], axis=1)
enlace
enlace=enlace.drop(enlace.columns[[25]], axis='columns')
enlace
#!pip install pandas --upgrade
from sqlalchemy import create_engine
engine = create_engine('mysql://ipxyz_tttctrl:Taquitos#938.$@108.179.194.48/ipxyz_nft_log')  
con = engine.connect()
enlace.to_sql(con=con, name='enlace', if_exists='append', chunksize=10000)
enlace.to_sql(con=con, name='enlaceactual', if_exists='replace', chunksize=10000)
print("se insertó el registro de forma correcta en enlace")


# In[ ]:




