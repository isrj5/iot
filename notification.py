# -*- encoding: utf-8 -*-
'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
''' Kisan Raja IOT Sensor API '''
import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
from datetime import datetime
from datetime import timedelta  
import logging
import json
import ast
import sys
import os
import time
import datainterface
import apistatic
from bson.objectid import ObjectId
import apilocale
import binascii

class Config(object):
    mongourl = 'localhost:27017'
    mongodb = 'kisanraja-dev-db'

    logname = 'notification.log'

    iotchannel = 'iot.kisanraja.com'
    iotport = 1883
    iotkeepalive = 60

    smsalerthours = 24
    dataalerthours = 6
    batteryVoltageAlert = 3.4
    batteryStopchargingVoltageAlert = 4.15    
    
    
class Static(object):
    logging.basicConfig(filename=Config.logname, level=logging.DEBUG)
    mongoclient = MongoClient(Config.mongourl)
    objAlert=datainterface.Alerts()
    objlang= apilocale.Language()
    mongodb = mongoclient[Config.mongodb]
    mongocollectionioterror = mongodb.ioterrors       
    mongocollectioniotalerts = mongodb.iotalerts

    mqttclient = mqtt.Client()    
    mqttclient.connect(Config.iotchannel, Config.iotport, Config.iotkeepalive)   

    devicehealthData = [] 
    smsgateways = {}
    smsAlerts = {}
    dataAlerts = {}
    userInfo = {}
    operators = {}
    subscribers = []
class DataAlert(object):    
    def sendDataAlert(self,message,userId):   
        message = message + '--Thank You, Kisan Raja'     
        print 'data message  : %s'%(message) 
        localmessage =  Static.objlang.get(message)             
        Static.mqttclient.publish(            
            "notifications/"+userId, localmessage, 1, retain=False)   
    def sendDeviceHealthMessage(self,flag,wuaName,spoutName,nodePermanentAddress,userName,userId,value):        
        message = 'Dear '+userName
        if flag == 5:
            message = message + ',Current Battery level = '+str(value)+'. Battery is Low. Please connect charger in '+wuaName +'/'+spoutName+' node '
        elif flag == 6:
            message = message + ',Current Battery level = '+str(value)+'. Battery is Full. Please remove charger in '+wuaName +'/'+spoutName+' node '
        self.sendDataAlert(message,userId)         
        Static.objAlert.insertAlert({'message':message, 'function-type':'devicehealth','notification-mode':'data','alert-type':flag,'send-time':datetime.now(),'permanentAddress':nodePermanentAddress})        
class SMS(object):    
    def sendSMS(self,msgs,mobile,gatewaySerialNo):                     
        for msg in msgs:            
            print 'send message to mobile %s message %s'%(mobile,msg)
            msg =  Static.objlang.gethex(msg)                     
            sensormessage = '{0:02x}'.format(ord('M'))    
            sensormessage = sensormessage + '{0:02x}'.format(1)                      
            sensormessage = sensormessage + '{0:02x}'.format(apistatic.getMotorControllerRequestFlag(apistatic.SMSTYPE))    
            sensormessage = sensormessage + '{0:02x}'.format(apistatic.GetMotorInfoDataTypeFlag('SENSORINFO'))          
            payloadsize =0             
            if apistatic.SMSTYPE == 'USERINFO':
                payloadsize = len(mobile)+ 1 + len(msg) + 2 + 1
            else:     
                payloadsize = len(mobile)+ 1 + len(msg)/2 + 2        
            sensormessage = sensormessage + '{0:04x}'.format(payloadsize)[2:]+'{0:04x}'.format(payloadsize)[:2]           
            for d in mobile:        
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
            Static.mqttclient.publish(
                "cloud/" + gatewaySerialNo, sensormessage, 1, retain=False)     
            time.sleep(apistatic.SMSWAITTIME)    
    def sendDeviceHealthMessage(self,flag ,wuaName,spoutName,nodePermanentAddress,userName,gatewaySerialNo,mobile,value):
        message = 'Dear '+userName
        messages = []
        if flag == 5:
            messages.append(message + ',Current Battery level = '+str(value)+'. Battery is Low.Please connect charger in '+wuaName +'/'+spoutName+' node ')
        elif flag == 6:
            messages.append(message + ',Current Battery level = '+str(value)+'. Battery is Full.Please remove charger in '+wuaName +'/'+spoutName+' node ')                      
        self.sendSMS(messages,mobile,gatewaySerialNo)        
        Static.objAlert.insertAlert({'message':messages,'gatewaySerialNo':gatewaySerialNo, 'function-type':'devicehealth','notification-mode':'sms','alert-type':flag,'send-time':datetime.now(),'permanentAddress':nodePermanentAddress})        
def lastsmsmessageDone(permanentAddress,alerttype):        
    if permanentAddress in Static.smsAlerts:
        if Static.smsAlerts[permanentAddress]==alerttype:
            return True
    return False
def lastDataNotificationDone(permanentAddress,alerttype):                    
    if permanentAddress in Static.dataAlerts:
        if Static.dataAlerts[permanentAddress]==alerttype:
            return True
    return False                
def processing():
    try:        
        objSMS=SMS()
        objDataAlert=DataAlert()           
        for healthData in  Static.devicehealthData:
            logging.debug('healthData for wuaId :%s wuaName:%s permanentAddress:%s nodeName :%s', healthData['wuaId'],healthData['wuaName'],healthData['permanentAddress'],healthData['nodeName'])   
            if healthData['delta1']==-1 or  healthData['delta2'] == -1:  
                alertrecord={'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'ERROR','wuaName':healthData['wuaName'],'sensors':healthData['sensors'],'message':'communication gap: failed in interval'}
                Static.mongodb.iotnotifications.update({'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'ERROR'},\
                            alertrecord,upsert=True)                                       
            else:
                interval = (healthData['delta1']-healthData['delta2']).total_seconds()    
                correction = min(60,healthData['setInterval']/10)
                logging.debug('interval:%d set interval :%d correction :%s',interval,healthData['setInterval'],correction)
                if not ( (healthData['setInterval']+correction) > interval and  (healthData['setInterval']-correction) < interval):
                    alertrecord={'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'ERROR', 'wuaName':healthData['wuaName'],'sensors':healthData['sensors'],'message':'communication gap: failed in interval'}
                    Static.mongodb.iotnotifications.update({'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'ERROR'}, alertrecord,upsert=True)   
                else:
                    Static.mongodb.iotnotifications.delete_one({'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'ERROR'})                            
                if healthData['node-battary-voltage'] < Config.batteryVoltageAlert: 
                    logging.debug('------node-battary-voltage %d',healthData['node-battary-voltage'])
                    alertrecord={'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'LOWBATTRY','wuaName':healthData['wuaName'],'sensors':healthData['sensors'],'message':'node has less battarey voltage'}
                    Static.mongodb.iotnotifications.update({'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'LOWBATTRY'},alertrecord,upsert=True)                                                                     
                    smsflag = lastsmsmessageDone(healthData['permanentAddress'],apistatic.ALRTTYPE.LOWBATTRY)                        
                    dataflag = lastDataNotificationDone(healthData['permanentAddress'],apistatic.ALRTTYPE.LOWBATTRY) 
                    userdata=Static.userInfo[Static.operators[healthData['wuaId']]]         
                    userdata['_id']=Static.operators[healthData['wuaId']]     
                    if not smsflag:                                                        
                        logging.debug('------ sms:sendDeviceHealthMessage sending now Flag:%d User Name :%s mobile:%s User id :%s',apistatic.ALRTTYPE.LOWBATTRY,userdata['name'],userdata['mobile'],userdata['_id'])
                        objSMS.sendDeviceHealthMessage(apistatic.ALRTTYPE.LOWBATTRY, healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],Static.smsgateways[healthData['wuaId']],userdata['mobile'],healthData['node-battary-voltage'])
                    if not dataflag:                        
                        logging.debug('------ data:sendDeviceHealthMessage sending now Flag :%d User Name :%s User id :%s',apistatic.ALRTTYPE.LOWBATTRY, userdata['name'],userdata['_id'])                                                                          
                        objDataAlert.sendDeviceHealthMessage(apistatic.ALRTTYPE.LOWBATTRY,healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],userdata['_id'],healthData['node-battary-voltage'])                                                                  
                    for uinfo in Static.subscribers:       
                        userdata=Static.userInfo[uinfo]         
                        userdata['_id']=uinfo     
                        if not smsflag:                                                        
                            logging.debug('------ sms:sendDeviceHealthMessage sending now Flag:%d User Name :%s mobile:%s User id :%s',apistatic.ALRTTYPE.LOWBATTRY,userdata['name'],userdata['mobile'],userdata['_id'])
                            objSMS.sendDeviceHealthMessage(apistatic.ALRTTYPE.LOWBATTRY, healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],Static.smsgateways[healthData['wuaId']],userdata['mobile'],healthData['node-battary-voltage'])
                        if not dataflag:                        
                            logging.debug('------ data:sendDeviceHealthMessage sending now Flag :%d User Name :%s User id :%s',apistatic.ALRTTYPE.LOWBATTRY, userdata['name'],userdata['_id'])                                                                          
                            objDataAlert.sendDeviceHealthMessage(apistatic.ALRTTYPE.LOWBATTRY,healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],userdata['_id'],healthData['node-battary-voltage'])
                elif healthData['node-battary-voltage'] > Config.batteryStopchargingVoltageAlert:
                    logging.debug('------node-battary-voltage %d',healthData['node-battary-voltage'])
                    alertrecord={'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'BATTRYFULL','wuaName':healthData['wuaName'],'sensors':healthData['sensors'],'message':'node battarey voltage is full'}                    
                    lbsmsflag = lastsmsmessageDone(healthData['permanentAddress'],apistatic.ALRTTYPE.LOWBATTRY)                        
                    lbdataflag = lastDataNotificationDone(healthData['permanentAddress'],apistatic.ALRTTYPE.LOWBATTRY)
                    fbsmsflag = lastsmsmessageDone(healthData['permanentAddress'],apistatic.ALRTTYPE.BATTRYFULL)                        
                    fbdataflag = lastDataNotificationDone(healthData['permanentAddress'],apistatic.ALRTTYPE.BATTRYFULL)                    
                    Static.mongodb.iotnotifications.delete_one({'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'LOWBATTRY'})                                                                                                   
                    userdata=Static.userInfo[Static.operators[healthData['wuaId']]]         
                    userdata['_id']=Static.operators[healthData['wuaId']]                    
                    if  lbsmsflag and not fbsmsflag:                                                        
                        logging.debug('------ sms:sendDeviceHealthMessage sending now Flag : %d User Name :%s mobile:%s User id :%s',apistatic.ALRTTYPE.BATTRYFULL,userdata['name'],userdata['mobile'],userdata['_id'])                                                                                                                                  
                        objSMS.sendDeviceHealthMessage(apistatic.ALRTTYPE.BATTRYFULL,healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],Static.smsgateways[healthData['wuaId']],userdata['mobile'],healthData['node-battary-voltage'])
                    if  lbdataflag and not fbdataflag:                        
                        logging.debug('------ data:sendDeviceHealthMessage sending now Flag : %d User Name :%s User id :%s',apistatic.ALRTTYPE.BATTRYFULL,userdata['name'],userdata['_id'])                                                                          
                        objDataAlert.sendDeviceHealthMessage(apistatic.ALRTTYPE.BATTRYFULL,healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],userdata['_id'],healthData['node-battary-voltage'])                    
                    for uinfo in Static.subscribers:       
                        userdata=Static.userInfo[uinfo]         
                        userdata['_id']=uinfo     
                        if  lbsmsflag and not fbsmsflag:                                                        
                            logging.debug('------ sms:sendDeviceHealthMessage sending now Flag : %d User Name :%s mobile:%s User id :%s',apistatic.ALRTTYPE.BATTRYFULL,userdata['name'],userdata['mobile'],userdata['_id'])                                                                                                                                  
                            objSMS.sendDeviceHealthMessage(apistatic.ALRTTYPE.BATTRYFULL,healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],Static.smsgateways[healthData['wuaId']],userdata['mobile'],healthData['node-battary-voltage'])
                        if  lbdataflag and not fbdataflag:                        
                            logging.debug('------ data:sendDeviceHealthMessage sending now Flag : %d User Name :%s User id :%s',apistatic.ALRTTYPE.BATTRYFULL,userdata['name'],userdata['_id'])                                                                          
                            objDataAlert.sendDeviceHealthMessage(apistatic.ALRTTYPE.BATTRYFULL,healthData['wuaName'],healthData['spoutName'] ,healthData['permanentAddress'],userdata['name'],userdata['_id'],healthData['node-battary-voltage'])                    
                else:
                    Static.mongodb.iotnotifications.delete_one({'wuaID':healthData['wuaId'],'permanentAddress':healthData['permanentAddress'],'notification':'LOWBATTRY'})                        
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'notification','module':'main.processing','error':e.message,'timestamp':datetime.now()}
        Static.mongodb.ioterrors.insert(error)                    
        logging.error('error %s',str(e.message))

def getDeviceHealth():    
    try:
        users = []        
        for wuarecord in Static.mongodb.iotconfigs.find({}):                     
            users.append(ObjectId(wuarecord['WUAOperator']))
            Static.operators[str(wuarecord['_id'])]=wuarecord['WUAOperator']
            for spout in wuarecord['spouts']:            
                for community in spout['communities']:            
                    for node in community['nodes']:                                
                        devicehealth ={}
                        devicehealth['wuaId'] = str(wuarecord['_id'])
                        devicehealth['wuaName'] = wuarecord['WUAName']
                        devicehealth['spoutName'] = spout['name']
                        devicehealth['WUAOperator'] = wuarecord['WUAOperator']                
                        devicehealth['nodeName']=node['name']
                        devicehealth['permanentAddress']= node['permanentAddress']                                                  
                        devicehealth['setInterval']= int(node['setInterval'])
                        devicehealth['sensors'] =':'
                        for sensor in node['sensors']:
                            devicehealth['sensors'] = devicehealth['sensors'] + sensor['name'] + ':'                
                        cnt=1
                        devicehealth['delta1']=-1
                        devicehealth['delta2']=-1                                
                        for iotrecord in Static.mongodb.iots.find({'data.nodePermanentAddress':devicehealth['permanentAddress']},{'createdAt':1,'data.sensors':1})\
                                                                .sort([('createdAt',-1)])\
                                                                .limit(2):                                       
                            devicehealth['delta'+str(cnt)]=iotrecord['createdAt']                      
                            if cnt == 1:
                                for sensor in iotrecord['data']['sensors']:                            
                                    if sensor['id']=='node-battary-voltage':                                    
                                        devicehealth['node-battary-voltage']=round(float(sensor['value'])/1000 ,2)
                            cnt =cnt + 1                                                                                                                                         
                        Static.devicehealthData.append(devicehealth)                
            for node in wuarecord['nodes']:         
                if node['type']=='MOTOR-CONTROL':
                    Static.smsgateways[devicehealth['wuaId']]=node['gatewaySerialNo']
                    continue;   
                devicehealth ={}
                devicehealth['wuaId'] = str(wuarecord['_id'])
                devicehealth['wuaName'] = wuarecord['WUAName']
                devicehealth['spoutName']=''
                devicehealth['WUAOperator'] = wuarecord['WUAOperator']               
                devicehealth['nodeName']=node['name']
                devicehealth['permanentAddress']= node['permanentAddress']                                
                devicehealth['setInterval']= int(node['setInterval'])
                devicehealth['sensors'] =':'
                for sensor in node['sensors']:
                    devicehealth['sensors'] = devicehealth['sensors'] + sensor['name'] + ':'
                cnt=1
                devicehealth['delta1']=-1
                devicehealth['delta2']=-1                        
                for iotrecord in Static.mongodb.iots.find({'data.nodePermanentAddress':devicehealth['permanentAddress']},{'createdAt':1,'data.sensors':1})\
                                                        .sort([('createdAt',-1)])\
                                                        .limit(2):                                       
                    devicehealth['delta'+str(cnt)]=iotrecord['createdAt']  
                    if cnt == 1:
                        for sensor in iotrecord['data']['sensors']:
                            if sensor['id']=='node-battary-voltage':
                                devicehealth['node-battary-voltage']=round(float(sensor['value'])/1000 ,2)
                    cnt =cnt + 1                                                                                                                                                                                                                                                                                                                                                          
                Static.devicehealthData.append(devicehealth)
                
        for subscriber in Static.mongodb.iotnotificationconfigs.find({'hardwareData':True}):                    
            users.append(ObjectId(subscriber['userId']))     
            Static.subscribers.append(str(subscriber['userId']))
        Static.userInfo= getUserDetails(users)           
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'notification','module':'getDeviceHealth','error':e.message,'timestamp':datetime.now()}
        Static.mongodb.ioterrors.insert(error)            
def getDeviceHealthAlert(mode,interval):
    try:
        alerts ={}                              
        today =  datetime.now()- timedelta(minutes=interval*60)                      
        for sendalerts in Static.mongodb.iotalerts.find({'function-type':'devicehealth','notification-mode':mode,'send-time':{'$gte':today}}):                                                                                                  
            alerts[sendalerts['permanentAddress']]=sendalerts['alert-type']                                                                                                                                                                
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'notification','module':'getDeviceHealthAlert','error':e.message,'timestamp':datetime.now()}
        Static.mongodb.ioterrors.insert(error)                           
    return alerts    
def getUserDetails(users):
    try:
        userDatapoints ={}                                                          
        for userInfo in Static.mongodb.users.find({'_id':{'$in':users}},{'_id':1,'mobile':1,'firstName':1}):                                                                                                                         
            userDatapoints[str(userInfo['_id'])]={}
            userDatapoints[str(userInfo['_id'])]['mobile']=userInfo['mobile']                       
            userDatapoints[str(userInfo['_id'])]['name']=userInfo['firstName']                                                                                                                                                      
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'notification','module':'getUserDetails','error':e.message,'timestamp':datetime.now()}
        self.mongodb.ioterrors.insert(error)                           
    return userDatapoints    
def display():
       print 'devicehealthData'
       print Static.devicehealthData   
       print 'userInfo'
       print Static.userInfo
       print 'OperatorInfo'
       print Static.operators
       print 'subscriberInfo'
       print Static.subscribers       
       print 'smsAlerts'
       print Static.smsAlerts
       print 'dataAlerts'
       print Static.dataAlerts   
       print 'smsgaetways'
       print Static.smsgateways
def main():
    try:
        Static.smsAlerts= getDeviceHealthAlert('sms',Config.smsalerthours)        
        Static.dataAlerts = getDeviceHealthAlert('data',Config.dataalerthours)        
        getDeviceHealth()
        display()
        processing()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'notification','module':'main','error':e.message,'timestamp':datetime.now()}
        Static.mongodb.ioterrors.insert(error)
def testmessage():    
    print 'Hi'
    msg =['This is the test message to make sure that everything works!']    
    objsms=SMS()            
    objsms.sendSMS(msg,'8310560902','SAM0565')    
if __name__ == '__main__':        
    #testmessage()
    main()
     
    