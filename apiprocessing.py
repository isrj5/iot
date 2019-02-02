# -*- encoding: utf-8 -*-
'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
''' Kisan Raja IOT Sensor API '''
import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
from datetime import datetime
import logging
import json
import ast
import sys
import os
import apistatic
import datainterface
import apilocale

class Config(object):
    mongourl = 'localhost:27017'
    mongodb = 'kisanraja-dev-db'

    iotchannel = 'iot.kisanraja.com'
    iotport = 1883
    iotkeepalive = 60

    logname = 'apiprocessing.log'


class Static(object):
    data = ''
    objlang = apilocale.Language()


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe(apistatic.PROCESSTOPIC + '#')


def sendUserInfoData(client,payload):        
    msgs = []                
    if payload['data-type'] == 'SENSORINFO':        
        msgs = diNotification.getSensorInfo(payload['farmer-mobile']);
    elif payload['data-type'] == 'IRRSCHEINFO':
        msgs = diNotification.getIrrigationSchedule(payload['farmer-mobile']);        
    elif payload['data-type'] == 'POWERINFO':
        msgs = diNotification.getPowerInfo(payload['farmer-mobile']);
    sendmessage =''        
    for msg in msgs:            
        print 'send message to mobile %s message %s'%(payload['farmer-mobile'],msg)
        sendmessage = sendmessage + msg
        msg =  Static.objlang.gethex(msg)                                   
        sensormessage = '{0:02x}'.format(ord(payload['sensor-type']))    
        sensormessage = sensormessage + '{0:02x}'.format(1)    
        sensormessage = sensormessage + '{0:02x}'.format(apistatic.getMotorControllerRequestFlag(apistatic.SMSTYPE))    
        sensormessage = sensormessage + '{0:02x}'.format(apistatic.GetMotorInfoDataTypeFlag(payload['data-type']))       
        payloadsize =0 
        if apistatic.SMSTYPE == 'USERINFO':
            payloadsize = len(payload['farmer-mobile'])+ 1 + len(msg) + 2 + 1
        else:     
            payloadsize = len(payload['farmer-mobile'])+ 1 + len(msg)/2 + 2         
        sensormessage = sensormessage + '{0:04x}'.format(payloadsize)[2:]+'{0:04x}'.format(payloadsize)[:2]           
        for d in payload['farmer-mobile']:        
            sensormessage = sensormessage + '{0:02x}'.format(ord(d))        
        sensormessage = sensormessage + '{0:02x}'.format(0)                         
        if apistatic.SMSTYPE == 'USERINFO':  
            sensormessage = sensormessage + '{0:04x}'.format(len(msg)+1)[2:]+'{0:04x}'.format(len(msg)+1)[:2]
            for dbyte in msg:
                sensormessage = sensormessage + '{0:x}'.format(ord(dbyte))          
            sensormessage = sensormessage + '{0:02x}'.format(0)                                                  
        else:                
            sensormessage = sensormessage + '{0:04x}'.format(len(msg)/2)[2:]+'{0:04x}'.format(len(msg)/2)[:2]            
            sensormessage = sensormessage + msg                  
        sensormessage = sensormessage.decode('hex')            
        client.publish(
            "cloud/" + payload['gatewaySerialNo'], sensormessage, 1, retain=False)             
    return sendmessage
def sendNotifications(client,payload):         
    UseConfig = diNotification.UserConfigbyNodeperAddr(payload['nodePermanentAddress'])    
    if len(UseConfig['users'])==0:
        return 'no Users'
    message = '<br>Water User Association : ' + UseConfig['WUAName'] +'<br />'
    message = message + 'Water User Association address : '+ UseConfig['WUAAdress']+'<br />'
    message = message + 'About Water User Association : '+ UseConfig['WUADescription']+'<br />'
    message = message + 'Data for :'+ UseConfig['scope']
    message = message +'<br />'        
    if payload['communication-type']=='NOTIFICATION-SENSOR-DATA':
        for sensordata in payload['sensors']:
            message = message +'<br />'+ sensordata['id']+' : '+str(sensordata['value']/sensordata['scale'])               
    elif payload['communication-type']=='NOTIFICATION-MOTOR-STATUS':   
        for mdata in payload['motor_data']:
            message = message +'<br />' + mdata['type'] + ':'+str(mdata['value']) + ' ' + mdata['units']
    elif payload['communication-type']=='NOTIFICATION-SENSOR-EVENT': 
        message = message +'<br /> new node added MAC :'+ payload['nodePermanentAddress']
    else:
        message = message +'<br /> not exptected.'
    print 'data message  : %s'%(message) 
    localmessage =  Static.objlang.get(message)        
    for userId in UseConfig['users']:	        
        client.publish(
        "notifications/"+userId, localmessage, 1, retain=False)
    return message    
def on_message(client, userdata, message):
    if apistatic.PROCESSTOPIC in message.topic:
        try:            
            iotrecod = {}
            iotrecod['topic'] = message.topic            
            iotrecod['createdAt'] = datetime.now();                            
            iotrecod['message'] = ast.literal_eval(message.payload)            
            payload = iotrecod['message']            
            if payload['communication-type'] == 'USERINFO':
                if payload['data-type'] == 'SENSORINFO'\
                   or payload['data-type'] == 'IRRSCHEINFO'\
                   or payload['data-type'] == 'POWERINFO':
                    iotrecod['sensor-type'] = payload['sensor-type']
                    iotrecod['farmer-mobile'] = payload['farmer-mobile']
                    iotrecod['send'] = sendUserInfoData(client,payload)
                    logging.debug(
                        'type : USERINFO, mobile  : %s', str(iotrecod))
                    mongocollectioniotprocessing.insert(iotrecod)
                else:
                    iotrecod['unknown'] = 'in type USERINFO undefined data-type'
                    logging.debug('unknow Record :%s', iotrecod)
                    mongocollectioniotuknown.insert(iotrecod)
            elif 'NOTIFICATION' in payload['communication-type']:                                             
                iotrecod['send'] = sendNotifications(client,payload)                     
                mongocollectioniotprocessing.insert(iotrecod)      
            else:
                iotrecod['unknown'] = 'processing : undefined req-type'
                logging.debug('unknow Record :%s', iotrecod)
                mongocollectioniotuknown.insert(iotrecod)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            iotrecod['error'] = e.message
            logging.error(e.message + '%s', iotrecod)
            mongocollectionioterror.insert(iotrecod)


logging.basicConfig(filename=Config.logname, level=logging.DEBUG)
diNotification = datainterface.Notification();
mqttclient = mqtt.Client()
mqttclient.on_connect = on_connect
mqttclient.on_message = on_message
mongoclient = MongoClient(Config.mongourl)
mongodb = mongoclient[Config.mongodb]
mongocollectioniotprocessing = mongodb.iotprocessings
mongocollectionioterror = mongodb.ioterrors
mongocollectioniotuknown = mongodb.iotunknowns
mqttclient.connect(Config.iotchannel, Config.iotport, Config.iotkeepalive)
mqttclient.loop_forever()
