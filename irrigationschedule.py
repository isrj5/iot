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
import time
import datainterface
import apistatic
from bson.objectid import ObjectId
import apilocale
class Config(object):
    mongourl = 'localhost:27017'
    mongodb = 'kisanraja-dev-db'

    logname = 'irrigationschedule.log'

    iotchannel = 'iot.kisanraja.com'
    iotport = 1883
    iotkeepalive = 60

    smsalerthours = 24
    dataalerthours = 6

    WLCOREECTION = 0.5

    NR = 9999
    
class Static(object):
    #Defining all the constants and variables that is to be used in the script

    logging.basicConfig(filename=Config.logname, level=logging.DEBUG)
    objirrsche = datainterface.IrrigationSchedule()
    objAlert=datainterface.Alerts()
    objlang = apilocale.Language()
    mongoclient = MongoClient(Config.mongourl)
    mongodb = mongoclient[Config.mongodb]
    mongocollectionioterror = mongodb.ioterrors       
    mongocollectioniotalerts = mongodb.iotalerts

    mqttclient = mqtt.Client()    
    mqttclient.connect(Config.iotchannel, Config.iotport, Config.iotkeepalive)    

    ScheduleSensorFarmerData= []
    motorData = {}
    smsAlerts = {}
    dataAlerts = {}
    userInfo = {}
    runMotor ={}
class DataAlert(object):
    #send alerts to the farmer when required
    def sendDataAlert(self,message,userId):   
        message = message + '--Thank You, Kisan Raja'      
        print ('data message : %s'%(message))
        localmessage =  Static.objlang.get(message) #to send notifiaction to the farmer in their choosen language  
        Static.mqttclient.publish(
            "notifications/"+userId, localmessage, 1, retain=False)   
    def sendStartMotorMessage(self,wuaId,wuaName,spoutName,userId):        
        message = 'Dear '+wuaName + ' pump operator,Water required in '+wuaName+'/'+spoutName+'.Please start the motor'          
        self.sendDataAlert(message,userId)        
        Static.objAlert.insertAlert({'message':message,'function-type':'irrsche','notification-mode':'data','alert-type':apistatic.ALRTTYPE.STARTMOTOR,'send-time':datetime.now(),'unitId':wuaId})
        setcurrentAlert('data',wuaId,apistatic.ALRTTYPE.STARTMOTOR)
    def sendStopMotorMessage(self,wuaId,wuaName,userId):
        message = 'Dear '+wuaName + ' pump operator,please stop the motor, all  spouts irrigation completed.'        
        self.sendDataAlert(message,userId)    
        Static.objAlert.insertAlert({'message':message,'function-type':'irrsche','notification-mode':'data','alert-type':apistatic.ALRTTYPE.STOPMOTOR,'send-time':datetime.now(),'unitId':wuaId})                
        setcurrentAlert('data',wuaId,apistatic.ALRTTYPE.STOPMOTOR)
    def sendStartirrigationMessage(self,communityId,userId):
        message = 'Dear '+Static.userInfo[userId]['name'] + ',less water level in the field .Please start irrigation'
        self.sendDataAlert(message,userId) 
        Static.objAlert.insertAlert({'message':message,'function-type':'irrsche','notification-mode':'data','alert-type':apistatic.ALRTTYPE.STARTIRRIGATION,'send-time':datetime.now(),'unitId':communityId})
        setcurrentAlert('data',communityId,apistatic.ALRTTYPE.STARTIRRIGATION)
    def sendExcessWaterMessage(self,communityId,userId):
        message = 'Dear '+Static.userInfo[userId]['name'] + ',excess water level in the field.Please remove water.'
        self.sendDataAlert(message,userId)
        Static.objAlert.insertAlert({'message':message,'function-type':'irrsche','notification-mode':'data','alert-type':apistatic.ALRTTYPE.EXCESSWATER,'send-time':datetime.now(),'unitId':communityId})
        setcurrentAlert('data',communityId,apistatic.ALRTTYPE.EXCESSWATER)
    def sendSufficientWaterMessage(self,communityId,userId):
        message = 'Dear '+Static.userInfo[userId]['name'] + ',please note : sufficient water level in field.'     
        self.sendDataAlert(message,userId)           
        Static.objAlert.insertAlert({'message':message,'function-type':'irrsche','notification-mode':'data','alert-type':apistatic.ALRTTYPE.SUFFICIENTWATER,'send-time':datetime.now(),'unitId':communityId})
        setcurrentAlert('data',communityId,apistatic.ALRTTYPE.SUFFICIENTWATER)
class SMS(object):    
    def sendSMS(self,msgs,mobile,gateWaySerialNo):               
        for msg in msgs:            
            print ('send message to mobile %s message %s'%(mobile,msg))
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
                "cloud/" + gateWaySerialNo, sensormessage, 1, retain=False)     
            time.sleep(apistatic.SMSWAITTIME)
    def sendStartMotorMessage(self,wuaId,wuaName,spoutName,gateWaySerialNo,userId):
        messages = ['Dear '+wuaName + ' pump operator,Water required in '+wuaName+'/'+spoutName+'.Please start the motor']
        mobile = Static.userInfo[userId]['mobile']           
        self.sendSMS(messages,mobile,gateWaySerialNo)
        Static.objAlert.insertAlert({'message':messages,'gatewaySerialNo':gateWaySerialNo,'function-type':'irrsche','notification-mode':'sms','alert-type':apistatic.ALRTTYPE.STARTMOTOR,'send-time':datetime.now(),'unitId':wuaId})
        setcurrentAlert('sms',wuaId,apistatic.ALRTTYPE.STARTMOTOR)
    def sendStopMotorMessage(self,wuaId,wuaName,gateWaySerialNo,userId):
        messages = ['Dear '+wuaName + ' pump operator,Please stop the motor, all  spouts irrigation completed.']
        mobile = Static.userInfo[userId]['mobile']           
        self.sendSMS(messages,mobile,gateWaySerialNo)        
        Static.objAlert.insertAlert({'message':messages,'gatewaySerialNo':gateWaySerialNo,'function-type':'irrsche','notification-mode':'sms','alert-type':apistatic.ALRTTYPE.STOPMOTOR,'send-time':datetime.now(),'unitId':wuaId})
        setcurrentAlert('sms',wuaId,apistatic.ALRTTYPE.STOPMOTOR)        
    def sendStartirrigationMessage(self,communityId,gateWaySerialNo,userId):
        messages = ['Dear '+Static.userInfo[userId]['name'] + ',less water level in the field .Please start irrigation']
        mobile = Static.userInfo[userId]['mobile']           
        self.sendSMS(messages,mobile,gateWaySerialNo)
        Static.objAlert.insertAlert({'message':messages,'gatewaySerialNo':gateWaySerialNo,'function-type':'irrsche','notification-mode':'sms','alert-type':apistatic.ALRTTYPE.STARTIRRIGATION,'send-time':datetime.now(),'unitId':communityId})
        setcurrentAlert('sms',communityId,apistatic.ALRTTYPE.STARTIRRIGATION)
    def sendExcessWaterMessage(self,communityId,gateWaySerialNo,userId):
        messages = ['Dear '+Static.userInfo[userId]['name'] + ',excess water level in the field.Please remove water.']
        mobile = Static.userInfo[userId]['mobile']           
        self.sendSMS(messages,mobile,gateWaySerialNo)        
        Static.objAlert.insertAlert({'message':messages,'gatewaySerialNo':gateWaySerialNo,'function-type':'irrsche','notification-mode':'sms','alert-type':apistatic.ALRTTYPE.EXCESSWATER,'send-time':datetime.now(),'unitId':communityId})
        setcurrentAlert('sms',communityId,apistatic.ALRTTYPE.EXCESSWATER)
    def sendSufficientWaterMessage(self,communityId,gateWaySerialNo,userId):
        messages = ['Dear '+Static.userInfo[userId]['name'] + ',please note : sufficient water level in field.']
        mobile = Static.userInfo[userId]['mobile']           
        self.sendSMS(messages,mobile,gateWaySerialNo)
        Static.objAlert.insertAlert({'message':messages,'gatewaySerialNo':gateWaySerialNo,'function-type':'irrsche','notification-mode':'sms','alert-type':apistatic.ALRTTYPE.SUFFICIENTWATER,'send-time':datetime.now(),'unitId':communityId})
        setcurrentAlert('sms',communityId,apistatic.ALRTTYPE.SUFFICIENTWATER)
def setcurrentAlert(alertflag ,unitId,alertType):
    if alertflag == 'sms':
        Static.smsAlerts[unitId]={}
        Static.smsAlerts[unitId][alertType]=True
    if alertflag == 'data':
        Static.dataAlerts[unitId]={}
        Static.dataAlerts[unitId][alertType]=True        
def lastsmsmessageDone(unitId,alerttype):  
    if unitId in Static.smsAlerts:
        if alerttype in Static.smsAlerts[unitId]:            
            return True
    return False              
def lastDataNotificationDone(unitId,alerttype):     
    if unitId in Static.dataAlerts:
        if alerttype in Static.dataAlerts[unitId]:            
            return True
    return False              
def processing():
    try:
        objSMS=SMS()
        objDataAlert=DataAlert()
        Static.ScheduleSensorFarmerData    = sorted(Static.ScheduleSensorFarmerData, key = lambda wuaid: (wuaid['wuaId'], wuaid['spoutId'],wuaid['sensorData']))    
        for schedule in  Static.ScheduleSensorFarmerData:     
            logging.debug('Schedule for wuaName :%s spoutName :%s community :%s sensorName:%s',schedule['wuaName'],schedule['spoutName'],schedule['communityName'],schedule['sensorName'])   
            if schedule['sensorName']=='water-level':       
                Static.runMotor[schedule['wuaId']]=False                  
            if schedule['minValue']==9999 or schedule['maxValue']==9999:                
                logging.debug('--Irrigation schedule is not applicable')
            elif schedule['sensorData']<schedule['minValue']:                                     
                logging.debug('--min value :%f > current value : %f',schedule['minValue'],schedule['sensorData']) 
                if schedule['sensorName']=='water-level':                           
                    Static.runMotor[schedule['wuaId']]=True                
                if schedule['wuaId'] in Static.motorData:
                    wuamotor=Static.motorData[schedule['wuaId']]                    
                    if wuamotor['simultaneous-spouts']>0:
                        logging.debug('----wuamotor wuaId :%s simultaneous-spouts: %d motor :%s',schedule['wuaId'],wuamotor['simultaneous-spouts'],wuamotor['MOTOR'])
                        Static.motorData[schedule['wuaId']]['simultaneous-spouts']=Static.motorData[schedule['wuaId']]['simultaneous-spouts']-1
                        if wuamotor['MOTOR'] =='OFF':  
                            smsflag = lastsmsmessageDone(schedule['wuaId'],apistatic.ALRTTYPE.STARTMOTOR)
                            dataflag = lastDataNotificationDone(schedule['wuaId'],apistatic.ALRTTYPE.STARTMOTOR)    
                            if not smsflag:
                                logging.debug('------ sms:sendStartMotorMeaage sending now  gateWaySerialNo:%s WUAOperator:%s', wuamotor['gateWaySerialNo'], wuamotor['WUAOperator'])
                                objSMS.sendStartMotorMessage(schedule['wuaId'],schedule['wuaName'],schedule['spoutName'],wuamotor['gateWaySerialNo'], wuamotor['WUAOperator'])
                            if not dataflag:
                                logging.debug('------ dataAlert:sendStartMotorMeaage  sending now WUAOperator:%s', wuamotor['WUAOperator'])                                                  
                                objDataAlert.sendStartMotorMessage(schedule['wuaId'],schedule['wuaName'],schedule['spoutName'],wuamotor['WUAOperator'])
                            if not smsflag or not dataflag:    
                                for farmer in schedule['farmers']:
                                    if not smsflag:
                                        logging.debug('------ sms:sendStartirrigationMessage sending now to famer :%s ',farmer)                                                  
                                        objSMS.sendStartirrigationMessage(schedule['communityId'],wuamotor['gateWaySerialNo'],farmer)    
                                    if not dataflag:
                                        logging.debug('------ dataAlert:sendStartirrigationMessage sending now to famer :%s ',farmer)
                                        objDataAlert.sendStartirrigationMessage(schedule['communityId'],farmer)                                                                        
                            else:
                                pass                                                         
                        else:                
                            smsflag =  lastsmsmessageDone(schedule['communityId'],apistatic.ALRTTYPE.STARTIRRIGATION)
                            dataflag =  lastDataNotificationDone(schedule['communityId'],apistatic.ALRTTYPE.STARTIRRIGATION)                            
                            if not smsflag or not dataflag:                                              
                                for farmer in schedule['farmers']:
                                    if not smsflag:
                                        logging.debug('------ sms:sendStartirrigationMessage sending now to famer :%s ',farmer)                                                  
                                        objSMS.sendStartirrigationMessage(schedule['communityId'],wuamotor['gateWaySerialNo'],farmer)
                                    if not dataflag:
                                        logging.debug('------ dataAlert:sendStartirrigationMessage sending now to famer :%s ',farmer)
                                        objDataAlert.sendStartirrigationMessage(schedule['communityId'],farmer)                                    
                            else:
                                pass                    
            elif schedule['sensorData']>schedule['maxValue']:
                if schedule['wuaId'] in Static.motorData:
                    wuamotor=Static.motorData[schedule['wuaId']]  
                    logging.debug('--maxValue :%f < current value : %f',schedule['maxValue'],schedule['sensorData'])                          
                    smsflag = lastsmsmessageDone(schedule['communityId'],apistatic.ALRTTYPE.EXCESSWATER)
                    dataflag = lastDataNotificationDone(schedule['communityId'],apistatic.ALRTTYPE.EXCESSWATER)                 
                    if not smsflag or not dataflag:
                        for farmer in schedule['farmers']:
                            if not smsflag:
                                logging.debug('------ sms:sendExcessWaterMessage sending now to famer :%s ',farmer)                                 
                                objSMS.sendExcessWaterMessage(schedule['communityId'],wuamotor['gateWaySerialNo'],farmer)   
                            if not dataflag:
                                logging.debug('------ dataAlert:sendExcessWaterMessage sending now to famer :%s ',farmer)
                                objDataAlert.sendExcessWaterMessage(schedule['communityId'],farmer)                              
            else:
                if schedule['wuaId'] in Static.motorData:
                    if schedule['sensorName']=='water-level':
                        if schedule['sensorData']  > (schedule['maxValue'] - Config.WLCOREECTION)  and schedule['sensorData']  < (schedule['maxValue'] - Config.WLCOREECTION):
                            wuamotor=Static.motorData[schedule['wuaId']]  
                            logging.debug('--min value :%f < current value :%f  >  max value : %f',schedule['minValue'],schedule['sensorData'],schedule['maxValue'])                     
                            smsirrifationflag = lastsmsmessageDone(schedule['communityId'],apistatic.ALRTTYPE.STARTIRRIGATION)
                            datairrigationflag = lastDataNotificationDone(schedule['communityId'],apistatic.ALRTTYPE.STARTIRRIGATION)  
                            smsdrainageflag =  lastsmsmessageDone(schedule['communityId'],apistatic.ALRTTYPE.EXCESSWATER) 
                            datadrainageflag =  lastDataNotificationDone(schedule['communityId'],apistatic.ALRTTYPE.EXCESSWATER) 
                            smsflag =  lastsmsmessageDone(schedule['communityId'],apistatic.ALRTTYPE.SUFFICIENTWATER) 
                            dataflag =  lastDataNotificationDone(schedule['communityId'],apistatic.ALRTTYPE.SUFFICIENTWATER)                    
                            if smsirrifationflag or datairrigationflag:                                              
                                for farmer in schedule['farmers']:
                                    if not smsflag:
                                        logging.debug('------ sms:sendSufficientWaterMessage sending now to famer :%s ',farmer) 
                                        objSMS.sendSufficientWaterMessage(schedule['communityId'],wuamotor['gateWaySerialNo'],farmer)     
                                    if not dataflag:
                                        logging.debug('------ dataAlert:sendSufficientWaterMessage sending now to famer :%s ',farmer) 
                                        objDataAlert.sendSufficientWaterMessage(schedule['communityId'],farmer)      
                            elif smsdrainageflag or datadrainageflag:                                              
                                for farmer in schedule['farmers']:
                                    if not smsflag:
                                        logging.debug('------ sms:sendSufficientWaterMessage sending now to famer :%s ',farmer)
                                        objSMS.sendSufficientWaterMessage(schedule['communityId'],wuamotor['gateWaySerialNo'],farmer)        
                                    if not dataflag:
                                        logging.debug('------ sms:sendSufficientWaterMessage sending now to famer :%s ',farmer)
                                        objDataAlert.sendSufficientWaterMessage(schedule['communityId'],farmer)    
                            else:
                                pass                                                                
        for schedule in  Static.ScheduleSensorFarmerData:
            if  schedule['wuaId'] in Static.motorData:
                wuamotor = Static.motorData[schedule['wuaId']]
                if not Static.runMotor[schedule['wuaId']] and wuamotor['MOTOR']=='ON':
                    smsflag = lastsmsmessageDone(schedule['wuaId'],apistatic.ALRTTYPE.STOPMOTOR)
                    dataflag = lastDataNotificationDone(schedule['wuaId'],apistatic.ALRTTYPE.STOPMOTOR)
                    if not smsflag:
                        objSMS.sendStopMotorMessage(schedule['wuaId'],schedule['wuaName'],wuamotor['gateWaySerialNo'],wuamotor['WUAOperator'])
                    if not dataflag:
                        objDataAlert.sendStopMotorMessage(schedule['wuaId'],schedule['wuaName'],wuamotor['WUAOperator'])                            
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'IrrigationSchedule','module':'main.processing','error':e.message,'timestamp':datetime.now()}
        Static.mongodb.ioterrors.insert(error)                    
        logging.error('error %s',str(e.message))
def getMotorData():
    Static.motorData=Static.objirrsche.getMotorData()
def getAlertData():
    Static.smsAlerts=Static.objirrsche.getIrrigationAlert('sms',Config.smsalerthours)        
    Static.dataAlerts=Static.objirrsche.getIrrigationAlert('data',Config.dataalerthours)        
def getUserData():
    users = []    
    for rec in Static.ScheduleSensorFarmerData:             
        for farmerId in rec['farmers']:
            users.append(ObjectId(farmerId))
    for wuaop in Static.motorData:        
        users.append(ObjectId(Static.motorData[wuaop]['WUAOperator']))        
    Static.userInfo = Static.objirrsche.getUserDetails(users)    
def getSchedule():    
    schedules = Static.objirrsche.getSchedule()
    Static.ScheduleSensorFarmerData=Static.objirrsche.getSensorAndFarmerData(schedules)  
def displayAll():
    print ('ScheduleSensorFarmerData')
    print (Static.ScheduleSensorFarmerData)
    print ('motorData')  
    print (Static.motorData)
    print  ('smsAlerts')
    print (Static.smsAlerts)
    print ('dataAlerts')
    print (Static.dataAlerts)
    print ('userInfo')
    print (Static.userInfo)     
    print ('--------------------------------------------------------------')        
def main():
    try:
        getSchedule()    
        getMotorData()
        getAlertData()
        getUserData()
        displayAll()
        processing()
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)                                    
        error={'class':'IrrigationSchedule','module':'main','error':e.message,'timestamp':datetime.now()}
        Static.mongodb.ioterrors.insert(error)
        logging.error('error %s',str(e.message))
if __name__ == '__main__':     
        logging.debug('-------------------------start-----------------------------')
        main()
     
    