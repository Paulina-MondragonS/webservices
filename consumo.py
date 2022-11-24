#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import threading
import time
while True:
    def ejecutar_doc1():
        exec(open(r"C:\webservices\enlace.py","r").read())
    hilo1 = threading.Thread(target=ejecutar_doc1)
    hilo1.start()
    
    def ejecutar_doc2():
        time.sleep(10)
        exec(open(r"C:\webservices\skybitz.py","r").read())
    hilo2 = threading.Thread(target=ejecutar_doc2)
    hilo2.start()
    
    def ejecutar_doc3():
        time.sleep(80)
        exec(open(r"C:\webservices\merge.py","r").read())
    hilo3 = threading.Thread(target=ejecutar_doc3)
    hilo3.start()
    
    def ejecutar_doc4():
        time.sleep(90)
        exec(open(r"C:\webservices\encontrac.py","r").read())
    hilo4 = threading.Thread(target=ejecutar_doc4)
    hilo4.start()
    
    def ejecutar_doc6():
        time.sleep(115)
        exec(open(r"C:\webservices\pointer.py","r").read())
    hilo6 = threading.Thread(target=ejecutar_doc6)
    hilo6.start()
    
    def ejecutar_doc7():
        time.sleep(125)
        exec(open(r"C:\webservices\nafta1.py","r").read())
    hilo7 = threading.Thread(target=ejecutar_doc7)
    hilo7.start()
    time.sleep(480)
    
    


# In[ ]:




