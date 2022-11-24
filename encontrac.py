#!/usr/bin/env python
# coding: utf-8

# In[5]:




import requests
#!pip install pymysql
#!pip install mysqlclient
#!pip install MySQL-python
#!pip install python3-mysqldb
#!pip3 install PyMySQL
import pymysql
pymysql.install_as_MySQLdb()
from urllib.request import urlopen
import json
import pandas as pd
from pandas import json_normalize
print("empieza a leer el codigo para insertar en encontrac")
datos= {"username":"DCAELPRNZC3DHJPCKXIXQ2","password":"key_WL3qojw3fo7W6PvHUUMER"}
response=requests.post('https://b2b-encontrack.com/api/rest/get/', data=datos)

j = response.json()
df = pd.DataFrame.from_dict(j)
df = df.drop(df.columns[[0,1,2,3,4,5,7,8,10,22,26]], axis='columns')
df
df.columns = [ 'idvehiculo', 'idequipo', 'latitude', 'longitude', 'speed','curso',
              'timestamp','messagereceivedtime','kilometraje','fposicionHcm', 'idtrayecto','notificacion',
              'idnotificacion', 'Noserie', 'placa','assetid',
              'idtipodeposicion','geopunto','estado', 'city','asentamiento', 'street','razonsocial']
df.dtypes
df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst = True)
df.dtypes
df['timestamp'] = pd.to_datetime(df['timestamp'],format="%Y-%m-%d")
df.dtypes
df['fecha'] = df['timestamp'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
#------1
df['messagereceivedtime'] = pd.to_datetime(df['messagereceivedtime'], dayfirst = True)
df.dtypes
df['messagereceivedtime'] = pd.to_datetime(df['messagereceivedtime'],format="%Y-%m-%d")
df.dtypes
df['fecha2'] = df['messagereceivedtime'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
#-------2
df['fposicionHcm'] = pd.to_datetime(df['fposicionHcm'], dayfirst = True)
df.dtypes
df['fposicionHcm'] = pd.to_datetime(df['fposicionHcm'],format="%Y-%m-%d")
df.dtypes
df['fecha3'] = df['fposicionHcm'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
#-------3
df2 = df.drop(df.columns[[6,7,9]], axis='columns')
df2
#---------------------quitar-05 de la fecha
#primertranform
df3=df2.loc[:,['fecha']]
df3
df3['dates'] = pd.to_datetime(df3['fecha']).dt.date
df3['times'] = pd.to_datetime(df3['fecha']).dt.time
df3
df3 = df3.drop(df3.columns[[0]], axis='columns')
df3
df5= df3['dates'].astype(str)
df5
df6= df3['times'].astype(str)
df6
df7=pd.concat([df5, df6], axis=1)
df7
df7["time"] = df7["dates"] +' '+df7["times"]
df7
df7 = df7.drop(df7.columns[[0,1]], axis='columns')
df7
#segundo dato
df4=df2.loc[:,['fecha2']]
df4
df4['dates'] = pd.to_datetime(df4['fecha2']).dt.date
df4['times'] = pd.to_datetime(df4['fecha2']).dt.time
df4
df4 = df4.drop(df4.columns[[0]], axis='columns')
df4
df8= df4['dates'].astype(str)
df8
df9= df4['times'].astype(str)
df9
df10=pd.concat([df8, df9], axis=1)
df10
df10["receivedtime"] = df10["dates"] +' '+df10["times"]
df10
df10 = df10.drop(df10.columns[[0,1]], axis='columns')
df10
#tercer dato
df11=df2.loc[:,['fecha3']]
df11
df11['dates'] = pd.to_datetime(df11['fecha3']).dt.date
df11['times'] = pd.to_datetime(df11['fecha3']).dt.time
df11
df11 = df11.drop(df11.columns[[0]], axis='columns')
df11
df12= df11['dates'].astype(str)
df12
df13= df11['times'].astype(str)
df13
df14=pd.concat([df12, df13], axis=1)
df14
df14["fposicionHcm"] = df14["dates"] +' '+df14["times"]
df14
df14 = df14.drop(df14.columns[[0,1]], axis='columns')
df14
encontrac = pd.concat([df2, df7,df10,df14], axis=1)
encontrac
encontrac=encontrac.drop(encontrac.columns[[20,21,22]], axis='columns')
encontrac
from sqlalchemy import create_engine
engine = create_engine('mysql://ipxyz_tttctrl:Taquitos#938.$@108.179.194.48/ipxyz_nft_log')  
con = engine.connect()
encontrac.to_sql(con=con, name='encontrac', if_exists='append', chunksize=10000)
encontrac.to_sql(con=con, name='encontracactual', if_exists='replace', chunksize=10000)
print("se insertó el registro de forma correcta en encontrac")


# In[ ]:




