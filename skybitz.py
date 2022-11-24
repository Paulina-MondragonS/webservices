#!/usr/bin/env python
# coding: utf-8

# In[3]:



#pip install mysqlclient
from sqlalchemy import create_engine
import pandas as pd
import time
from numpy import random
import requests
from datetime import datetime
from dateutil import tz
import pytz
from datetime import datetime, tzinfo
from dateutil import tz
import pytz
print("empieza a leer el codigo para insertar en skybitz")
response = requests.get("https://xml.skybitz.com:9443/QueryPositions?assetid=all&customer=TolucaXmL13220&password=Lpt62wRreV4Ndvp&version=2.72")
response.content
contenido = response.content

#print(type(response.content))
json_values =response
import xml.etree.ElementTree as ET
e = ET.ElementTree(ET.fromstring(contenido))
raiz = e.getroot()
a= raiz.tag
b=  raiz.attrib
    
    
data = []
bandera_maxSeat = None
registro = [None, None, None, None,None,None,None,None,None, None,None,None,None,None]
contador = 0 
for fligh in raiz:
    if fligh.tag == 'gls':
        registro[12]= contador
        
        for hijo2 in fligh:
            if hijo2.tag == 'asset':
                for hijoSegment in hijo2:
                    if hijoSegment.tag == 'assetid':
                        registro[0]= hijoSegment.text
                    if hijoSegment.tag == 'owner':
                        registro[1]= hijoSegment.text
            if hijo2.tag == 'latitude':
                    registro[2]= hijo2.text
            if hijo2.tag == 'longitude':
                    registro[3]= hijo2.text
            if hijo2.tag == 'speed':
                    registro[4]= hijo2.text
            if hijo2.tag == 'battery':
                    registro[5]= hijo2.text
                    
            if hijo2.tag == 'idle':
                for hijoSegment in hijo2:
                    if hijoSegment.tag == 'idlestatus':
                        registro[6]= hijoSegment.text
                    if hijoSegment.tag == 'idleduration':
                        registro[7]= hijoSegment.text
            if hijo2.tag == 'landmark':
                for hijoSegment in hijo2:
                    if hijoSegment.tag == 'city':    
                        registro[8]= hijoSegment.text
                    if hijoSegment.tag == 'postal':
                        registro[9]= hijoSegment.text
            if hijo2.tag == 'address':
                for hijoSegment in hijo2:
                    if hijoSegment.tag == 'street':    
                        registro[10]= hijoSegment.text
              
            if hijo2.tag == 'time':
                    registro[11]= hijo2.text           
            if hijo2.tag == 'messagereceivedtime':
                    registro[12]= hijo2.text
            if hijo2.tag == 'devicetype':
                    registro[13]= hijo2.text
                
        data.append(tuple(registro))
        registro = [None, None, None, None, None,None, None, None,None,None, None,None,None,None]
    
        contador = contador +1


        for valores in data:
                #print(valores)

#convierte los datos en un frame
            df = pd.DataFrame(data)
                #data_df 
    
#nombra las columnas
df.columns = [ 'assetid', 'razonsocial', 'latitude', 'longitude', 'speed', 'battery','status','idleduration','city','postal','street','timestamp', 'messagereceivedtime','devicetype']
#elimina la primer fila para limpiar los datos
    #data_df = data_df.iloc[1: , :]
df.dtypes
df['timestamp'] = pd.to_datetime(df['timestamp'],format="%Y-%m-%d")
df['fecha'] = df['timestamp'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
df['messagereceivedtime'] = pd.to_datetime(df['messagereceivedtime'],format="%Y-%m-%d")
df['fecha2'] = df['messagereceivedtime'].dt.tz_localize('utc').dt.tz_convert('America/Mexico_City')
df
df2 = df.drop(df.columns[[11,12]], axis='columns')
df2
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
skybitz = pd.concat([df, df7,df10], axis=1)
skybitz
skybitz=skybitz.drop(skybitz.columns[[11,12,14,15]], axis='columns')
skybitz
from sqlalchemy import create_engine
engine = create_engine('mysql://ipxyz_tttctrl:Taquitos#938.$@108.179.194.48/ipxyz_nft_log')  
con = engine.connect()
skybitz.to_sql(con=con, name='skybitz', if_exists='append', chunksize=10000)
skybitz.to_sql(con=con, name='skybitzactual', if_exists='replace', chunksize=10000)
print("se insertó el registro de forma correcta en skybitz")


# In[ ]:




