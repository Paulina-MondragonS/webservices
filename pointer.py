#!/usr/bin/env python
# coding: utf-8

# In[4]:



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
print("comienza a leer codigo para insertar en pointer")
datos= {"Usuario":"TransportesT","Password":"TTSchneider9"}
response=requests.post('http://wsprmx.pointer.mx/trattosa/wstrattosa.asmx/ConsultaUltimaPosicion', data=datos)
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
registro = [None, None, None, None,None,None]
contador = 0 
for fligh in raiz:
    if fligh.tag == 'Posicion':
        registro[5]= contador
        
        for hijo2 in fligh:
            if hijo2.tag == 'IdEquipo':
                    registro[0]= hijo2.text
            if hijo2.tag == 'Economico':
                    registro[1]= hijo2.text
            if hijo2.tag == 'FechaHora':
                    registro[2]= hijo2.text
            if hijo2.tag == 'Latitud':
                    registro[3]= hijo2.text
            if hijo2.tag == 'Longitud':
                    registro[4]= hijo2.text
            if hijo2.tag == 'Direccion':
                    registro[5]= hijo2.text           
        data.append(tuple(registro))
        registro = [None, None, None, None, None,None]
        contador = contador +1
        for valores in data:
                #print(valores)
#convierte los datos en un frame
            df = pd.DataFrame(data)
                #data_df 
#nombra las columnas
df.columns = [ 'idequipo', 'assetid', 'time', 'latitude', 'longitude', 'street']
df
df2=pd.to_datetime(df.time).dt.strftime('%Y-%m-%d %H:%M:%S')
df2
pointer = pd.concat([df, df2], axis=1)
pointer
pointer.columns = [ 'idequipo', 'assetid', 'time_fix', 'latitude', 'longitude', 'street','time']
pointer
pointer= pointer.drop(pointer.columns[[2]], axis='columns')
pointer
from sqlalchemy import create_engine
engine = create_engine('mysql://ipxyz_tttctrl:Taquitos#938.$@108.179.194.48/ipxyz_nft_log')  
con = engine.connect()
pointer.to_sql(con=con, name='pointer', if_exists='append', chunksize=10000)
pointer.to_sql(con=con, name='pointeractual', if_exists='replace', chunksize=10000)
print("se insertó el registro de forma correcta en pointer")


# In[ ]:




