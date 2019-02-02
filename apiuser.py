'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
''' Kisan Raja IOT Sensor API '''
import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
from datetime import datetime
import logging
import json
import ast
import krcrc16

class Config(object):
    mongourl ='localhost:27017'
    mongodb='kisanraja-dev-db'

    iotchannel ='iot.kisanraja.com'
    iotport = 1883
    iotkeepalive=60

    logname= 'apiusers.log'

class Static(object):
    data=''
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("user/#")

def on_message(client, userdata, message):                            
    if 'user/' in message.topic:                
        try:
            iotrecod = {}
            iotrecod['topic']=message.topic            
            iotrecod['createdAt']=datetime.now()                         
            payload=ast.literal_eval(message.payload)
            iotrecod['message']=payload                        
            if payload['sensorType'] == 'M':                            
                if payload['type'] =='COMMAND':                    
                    sensormessage= '{0:02x}'.format(ord(payload['sensorType']))                          
                    sensormessage= sensormessage + '{0:02x}'.format(int(payload['shortAddress']))                         
                    sensormessage= sensormessage + '{0:02x}'.format(payload['requesttype'])                                        
                    sensormessage= sensormessage + '{0:02x}'.format(4+payload['command'])                                        
                    sensormessage= sensormessage + '0100'                    
                    sensormessage= sensormessage + '{0:02x}'.format(4+payload['command'])                                        
                    iotrecod['sensormessage']=sensormessage                                      
                    sensormessage = sensormessage.decode('hex')                       
                    client.publish("cloud/"+payload['gatewaySerialNo'],sensormessage,1,retain=False)
                    logging.debug('MOTOR COMMAND : TYPE : M : final Record :%s',str(iotrecod)) 
                    mongocollectioniotuser.insert(iotrecod)
                elif payload['type'] =='STATUS':
                    print payload['type']
                else:
                    iotrecod['unknown']='MOTOR CONTROL TYPE NOT DEFINED.'
                    logging.debug('unknow Record :%s',iotrecod)                                     
                    mongocollectioniotuknown.insert(iotrecod)                                                            
            elif payload['sensorType'] == 'S':
                if payload['type'] == 'SET_INTERVAL':                                               
                    sensormessage= '{0:02x}'.format(ord(payload['sensorType']))
                    sensorpayload = '{0:04x}'.format(payload['shortAddress']) +'071f02'+ '{0:04x}'.format(payload['interval'])                    
                    crc16 =krcrc16.crc16(sensorpayload,len(sensorpayload)/2)
                    sensorpayloadcrc = '{:04x}'.format(crc16)[-4:]
                    header ='000500000007fff3'+sensorpayloadcrc
                    sensormessage= sensormessage + header+ sensorpayload       
                    iotrecod['sensormessage']=sensormessage
                    sensormessage = sensormessage.decode('hex')                                          
                    client.publish("cloud/"+payload['gatewaySerialNo'],sensormessage,1,retain=False)
                    logging.debug('SET_INTERVAL : TYPE : S : final Record :%s',str(iotrecod)) 
                    mongocollectioniotuser.insert(iotrecod)
                elif payload['type'] == 'RESTART_CORDINATOR': 
                    sensormessage= '{0:02x}'.format(ord(payload['sensorType']))
                    sensormessage =sensormessage+'000b00000000fff40000'
                    iotrecod['sensormessage']=sensormessage
                    sensormessage = sensormessage.decode('hex')                                                    
                    client.publish("cloud/"+payload['gatewaySerialNo'],sensormessage,1,retain=False)         
                    logging.debug('RESTART_CORDINATOR: final Record :%s',str(iotrecod)) 
                    mongocollectioniotuser.insert(iotrecod)                      
                elif    payload['type'] == 'GET_ATTRIBUTE_VALUE':  
                    sensormessage= '{0:02x}'.format(ord(payload['sensorType']))
                    sensorpayload = '{0:04x}'.format(payload['shortAddress']) +'0f41020208'
                    crc16 =krcrc16.crc16(sensorpayload,len(sensorpayload)/2)
                    sensorpayloadcrc = '{:04x}'.format(crc16)[-4:]
                    header ='000500000007fff3'+sensorpayloadcrc
                    sensormessage= sensormessage + header+ sensorpayload       
                    iotrecod['sensormessage']=sensormessage
                    print sensormessage
                    sensormessage = sensormessage.decode('hex')                                          
                    client.publish("cloud/"+payload['gatewaySerialNo'],sensormessage,1,retain=False)
                    logging.debug('GET_ATTRIBUTE_VALUE : final Record :%s',str(iotrecod)) 
                    mongocollectioniotuser.insert(iotrecod)                    
                elif    payload['type'] == 'SET_ATTRIBUTE_VALUE':  
                    sensormessage= '{0:02x}'.format(ord(payload['sensorType']))                       
                    sensorpayload = '{0:04x}'.format(payload['shortAddress']) +'104308410202084202'+ '{0:04x}'.format(payload['attrvalue'])                    
                    crc16 =krcrc16.crc16(sensorpayload,len(sensorpayload)/2)
                    sensorpayloadcrc = '{:04x}'.format(crc16)[-4:]
                    header ='00050000000dffed'+sensorpayloadcrc
                    sensormessage= sensormessage + header+ sensorpayload       
                    iotrecod['sensormessage']=sensormessage
                    print sensormessage
                    sensormessage = sensormessage.decode('hex')                                          
                    client.publish("cloud/"+payload['gatewaySerialNo'],sensormessage,1,retain=False)
                    logging.debug('GET_ATTRIBUTE_VALUE : final Record :%s',str(iotrecod)) 
                    mongocollectioniotuser.insert(iotrecod)                    
                else:                      
                    iotrecod['unknown']='undefined message type'
                    logging.debug('unknow Record :%s',str(iotrecod)) 
                    mongocollectioniotuknown.insert(iotrecod)                                        
            else:
                iotrecod['unknown']='undefined sensor type'
                logging.debug('unknow Record :%s',str(iotrecod)) 
                mongocollectioniotuknown.insert(iotrecod)                                                                       
        except Exception as e:            
            iotrecod['error']=e.message         
            logging.error(str(e.message)+'%s',str(iotrecod)) 
            mongocollectionioterror.insert(iotrecod) 

logging.basicConfig(filename=Config.logname,level=logging.DEBUG)
mqttclient = mqtt.Client()
mqttclient.on_connect = on_connect
mqttclient.on_message = on_message
mongoclient=MongoClient(Config.mongourl)
mongodb=mongoclient[Config.mongodb]
mongocollectioniotuser=mongodb.iotusers
mongocollectionioterror=mongodb.ioterrors
mongocollectioniotuknown=mongodb.iotunknowns
mqttclient.connect(Config.iotchannel, Config.iotport, Config.iotkeepalive)
mqttclient.loop_forever()

