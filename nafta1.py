#!/usr/bin/env python
# coding: utf-8

# In[9]:



from urllib.request import urlopen
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
print("comienza el registro de forma correcta en nafta1")
url = "https://api-stg.gpsinsight.com/v2/vehicle/location?channel=example&session_token=5e6464d9762501dbbfc76ac8560fbeb949ff1eab1c58e60265f5"
response = urlopen(url)
data_json = json.loads(response.read())
dfjson=json_normalize(data_json['data'])
dfjson
dficon=dfjson.loc[:,['speed_icon']]
dficon
dficon = dficon["speed_icon"].replace({'bright_':''}, regex=True)
dficon
dficon= dficon.replace({'_':'-'}, regex=True)
df=dfjson
#-------- fix_time
df['fix_time'] = pd.to_datetime(df['fix_time'], dayfirst = True)
df
df['fix_time'] = pd.to_datetime(df['fix_time'],format="%Y-%m-%d")
df
df['fix_time'] = df['fix_time'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
#---------fix_time_mst
df['fix_time_mst'] = df['fix_time_mst'].str.replace('MST', '', regex=True)
df
df['fix_time_mst'] = pd.to_datetime(df['fix_time_mst'], dayfirst = True)
df
df['fix_time_mst'] = pd.to_datetime(df['fix_time_mst'],format="%Y-%m-%d")
df
df['fix_time_mst'] = df['fix_time_mst'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
#---------fix_time_gmt
df['fix_time_gmt'] = pd.to_datetime(df['fix_time_gmt'], dayfirst = True)
df
df['fix_time_gmt'] = pd.to_datetime(df['fix_time_gmt'],format="%Y-%m-%d")
df
df['fix_time_gmt'] = df['fix_time_gmt'].dt.tz_convert('America/Mexico_City')
df
#---------exec_time
df['exec_time'] = pd.to_datetime(df['exec_time'], dayfirst = True)
df
df['exec_time'] = pd.to_datetime(df['exec_time'],format="%Y-%m-%d")
df
df['exec_time'] = df['exec_time'].dt.tz_convert('America/Mexico_City')
df
#------ping_time_utf
df['ping_time_utf'] = pd.to_datetime(df['ping_time_utf'], dayfirst = True)
df
df['ping_time_utf'] = pd.to_datetime(df['ping_time_utf'],format="%Y-%m-%d")
df
df['ping_time_utf'] = df['ping_time_utf'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
#########tranformacion 
#----------fix_time TRANSFORMACION
df3=df.loc[:,['fix_time']]
df3
df3['dates'] = pd.to_datetime(df3['fix_time']).dt.date
df3['times'] = pd.to_datetime(df3['fix_time']).dt.time
df3
df3 = df3.drop(df3.columns[[0]], axis='columns')
df3
df5= df3['dates'].astype(str)
df5
df6= df3['times'].astype(str)
df6
df7=pd.concat([df5, df6], axis=1)
df7
df7["fix_time"] = df7["dates"] +' '+df7["times"]
df7
df7 = df7.drop(df7.columns[[0,1]], axis='columns')
df7
#----------fix_time_mst TRANSFORMACION
df4=df.loc[:,['fix_time_mst']]
df4
df4['dates'] = pd.to_datetime(df4['fix_time_mst']).dt.date
df4['times'] = pd.to_datetime(df4['fix_time_mst']).dt.time
df4
df4 = df4.drop(df4.columns[[0]], axis='columns')
df4
df8= df4['dates'].astype(str)
df8
df9= df4['times'].astype(str)
df9
df10=pd.concat([df8, df9], axis=1)
df10
df10["fix_time_mst"] = df10["dates"] +' '+df10["times"]
df10
df10 = df10.drop(df10.columns[[0,1]], axis='columns')
df10
#----------fix_time_gmt TRANSFORMACION
df11=df.loc[:,['fix_time_gmt']]
df11
df11['dates'] = pd.to_datetime(df11['fix_time_gmt']).dt.date
df11['times'] = pd.to_datetime(df11['fix_time_gmt']).dt.time
df11
df11 = df11.drop(df11.columns[[0]], axis='columns')
df11
df12= df11['dates'].astype(str)
df12
df13= df11['times'].astype(str)
df13
df14=pd.concat([df12, df13], axis=1)
df14
df14["fix_time_gmt"] = df14["dates"] +' '+df14["times"]
df14
df14= df14.drop(df14.columns[[0,1]], axis='columns')
df14
#----------exec_time TRANSFORMACION
df15=df.loc[:,['exec_time']]
df15
df15['dates'] = pd.to_datetime(df15['exec_time']).dt.date
df15['times'] = pd.to_datetime(df15['exec_time']).dt.time
df15
df15 = df15.drop(df15.columns[[0]], axis='columns')
df15
df16= df15['dates'].astype(str)
df16
df17= df15['times'].astype(str)
df17
df18=pd.concat([df16, df17], axis=1)
df18
df18["exec_time"] = df18["dates"] +' '+df18["times"]
df18
df18= df18.drop(df18.columns[[0,1]], axis='columns')
df18
#----------ping_time_utf TRANSFORMACION
df19=df.loc[:,['ping_time_utf']]
df19
df19['dates'] = pd.to_datetime(df19['ping_time_utf']).dt.date
df19['times'] = pd.to_datetime(df19['ping_time_utf']).dt.time
df19
df19 = df19.drop(df19.columns[[0]], axis='columns')
df19
df20= df19['dates'].astype(str)
df20
df21= df19['times'].astype(str)
df21
df22=pd.concat([df20, df21], axis=1)
df22
df22["ping_time_utf"] = df22["dates"] +' '+df22["times"]
df22
df22= df22.drop(df22.columns[[0,1]], axis='columns')
df22
nafta = pd.concat([df, df7,df10,df14,df18,df22,dficon], axis=1)
nafta.columns=['id', 'vin', 'assetid', 'serial_number', 'serial_number_label',
       'age_minutes', 'timezone', '1', '2', '3',
       '4', '5', 'latitude', 'longitude', 'street',
       'heading', 'ignition', 'max_speed', 'avg_speed', 'inst_speed',
       'speed_limit', 'speed_label', 'speed_icon', 'odometer', 'voltage',
       'driver_id', 'inputs', 'inputs_on','direction', 'fix_time', 'fix_time_mst',
       'time', 'exec_time', 'ping_time_utf', 'speed_icons']
nafta
nafta
nafta.drop(['1','2','3','4','5','direction','inputs_on','inputs','direction','speed_icon'], axis = 'columns', inplace=True)
nafta=nafta[['id', 'vin', 'assetid', 'serial_number', 'serial_number_label',
       'age_minutes', 'timezone', 'latitude', 'longitude', 'street',
       'heading', 'ignition', 'max_speed', 'avg_speed', 'inst_speed',
       'speed_limit', 'speed_label', 'odometer', 'voltage',
       'driver_id','speed_icons', 'fix_time', 'fix_time_mst',
       'time', 'exec_time', 'ping_time_utf']]


nafta
from sqlalchemy import create_engine
engine = create_engine('mysql://ipxyz_tttctrl:Taquitos#938.$@108.179.194.48/ipxyz_nft_log')  
con = engine.connect()
nafta.to_sql(con=con, name='nafta', if_exists='append', chunksize=10000)
nafta.to_sql(con=con, name='naftaactual', if_exists='replace', chunksize=10000)
print("se insertó el registro de forma correcta en nafta1")




# In[ ]:




