from pymongo import MongoClient
import json
from datetime import datetime
from datetime import timedelta  
from bson.objectid import ObjectId
import logging
import json
import ast
import sys
import os
import apiutils
import apistatic

class Config(object):
    mongourl ='localhost:27017'
    mongodb='kisanraja-dev-db'
    
   

class Notification(object):
    mongoclient=MongoClient(Config.mongourl)
    mongodb=mongoclient[Config.mongodb]
    def sensorinforbyuserId(self,userId,daydate):        
        result = {'water-level':-1,'soil-moisture':-1}
        try:            
            records = self.mongodb.iotconfigs.find({"spouts.communities.farmers":{"$elemMatch":{"userId":str(userId)}}})                                
            for rec in records:                                                                                    
                for spout in rec['spouts']:
                    for community in spout['communities']:                     
                        for uId in community['farmers']:      
                            if str(userId) == str(uId['userId']):                                                               
                                for node in community['nodes']:                                                                                              
                                    addons={'consc':1,'consk':1,'conswet':1,'conssoilmax':1}
                                    for sensor in node['sensors']:                                                                                                                         
                                        if sensor['name']== 'water-level':                                                                    
                                            for addson in sensor['addOns']:                                                
                                                if addson['field']=='Consc':
                                                    addons['consc']= addson['value']  
                                                if addson['field']=='Consk':
                                                    addons['consk']= addson['value'] 
                                        if sensor['name']== 'soil-moisture':                                            
                                            for addson in sensor['addOns']:                                                
                                                if addson['field']=='conswet':
                                                    addons['conswet']= addson['value']  
                                                if addson['field']=='consmax':
                                                    addons['conssoilmax']= addson['value']                                                                                                                                                                                                                              
                                    address =  node['permanentAddress']                                           
                                    nodeSensorData = self.mongodb.iots.find({ 'data.nodePermanentAddress': str(address)})\
                                                            .sort([('createdAt',-1)])\
                                                            .limit(1)                                                                                                                              
                                    for sd in nodeSensorData:                                        
                                        for sensordata in sd['data']['sensors']:
                                            if sensordata['id'] == 'water-level':                                                             
                                                result['water-level'] =apiutils.calibrate('water-level',sensordata['value'],addons)                                            
                                            elif sensordata['id'] == 'soil-moisture':                                                                                                                                                 
                                                result['soil-moisture'] =apiutils.calibrate('soil-moisture',sensordata['value'],addons)
        except Exception as e:
            message = message + ',sorry unable to process you message.'
            error={'class':'Notification','module':'sensorinforbyuserId','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)       
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                                                                        
        return result                                            
        
    def getIrrigationSchedule(self,mobilenum):     
        messages =[]
        message =''   
        user_id=self.mongodb.users.find_one({'mobile':mobilenum},{'_id':1,'firstName':1})                    
        if user_id is not None:
            message = message + user_id['firstName']
            records = self.mongodb.iotconfigs.find({"spouts.communities.farmers":{"$elemMatch":{"userId":str(user_id['_id'])}}},{'spouts.$':1})                          
            for rec in records:                                                
                for spout in rec['spouts']:         
                    for community in spout['communities']:
                        for uId in community['farmers']:      
                            if str(user_id['_id']) == str(uId['userId']):                 
                                today =  datetime.now()        
                                today=today.replace(hour=0, minute=0, second=0, microsecond=0)        
                                irrsches = self.mongodb.irrigationschedules.find({'wuaId':str(rec['_id']),'spoutId':str(spout['_id']),'communityId':str(community['_id']),'sowingStartDate':{'$lte':today},'sowingEndDate':{'$gte':today},'schedule.day':today},{'sensorName':1,'schedule.$':1})                                                                                                              
                                sensordata = self.sensorinforbyuserId(user_id['_id'])                                
                                flag = 0
                                for schedules in irrsches:  
                                    minValue = float(schedules['schedule'][0]['minValue'])
                                    maxValue = float(schedules['schedule'][0]['maxValue'])
                                    if schedules['sensorName'] == 'water-level':                                                                                
                                        if maxValue>float(sensordata['water-level']) and minValue<float(sensordata['water-level']):
                                            flag = 1
                                        elif  maxValue<float(sensordata['water-level']):
                                            flag = 2
                                            break;
                                        else:
                                            flag =3        
                                            break;                                
                                    if schedules['sensorName'] == 'water-level': 
                                        if maxValue>float(sensordata['soil-moisture']) and minValue<float(sensordata['soil-moisture']):
                                            flag = 1
                                        elif  maxValue<float(sensordata['soil-moisture']):
                                            flag = 2
                                            break;
                                        else:
                                            flag =3        
                                            break;                                        
                                if flag ==0 :
                                    messages.append(message +', For 0 to 25 days - " Irrgation schedule not applicable in this stage " or " Irrigation schedule not applicable till day 25 " . ')
                                    messages.append('(This scenario will vary from crop to crop and also depending upon direct seeded rice and nursery transplanted rice).')   
                                elif flag == 1:
                                    messages.append(message +', When sensor values are OK -" Your farm Soil moisture = '+str(round(sensordata['soil-moisture'],2))+'%,Water level = '+str(round(sensordata['soil-moisture'],2))+'cm, Irrigation is as expected ".')
                                elif flag == 2:
                                    messages.append(message +', When Sensor values are above required value - " Your farm Soil moisture = '+str(round(sensordata['soil-moisture'],2))+'%,Water level = '+str(round(sensordata['water-level'],2))+'cm,  kindly Check drainage alert SMS received ".')                                       
                                elif flag==3:
                                    messages.append(message +', When sensor values are below required value - " Your farm Soil moisture = '+str(round(sensordata['soil-moisture'],2))+'%,Water level = '+str(round(sensordata['water-level'],2))+'cm,  kindly Check Irrigation alert SMS received ".')                                                                                                                 
        return messages
    def getSensorInfo(self,mobilenum): 
        messages = []       
        message =''
        try:
            user_id=self.mongodb.users.find_one({'mobile':mobilenum},{'_id':1,'firstName':1})                    
            if user_id is not None:
                message = message + user_id['firstName']                               
                result = self.sensorinforbyuserId(user_id['_id'])                
                message = message + ',' + 'soil-moisture : '+ str(round(result['soil-moisture'],2)) + '% and '+ 'water-level : '+str(round(result['water-level'],2))+'cm'                
            else:
                message = message + ' please do registration devcrm.kisanraja.com as farmer.'                                         
        except Exception as e:
            message = message + ',sorry unable to process you message.'
            error={'mobile':mobilenum,'type':'USERINFO','req-type':'SENSORINFO','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)       
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                
        messages.append(message[0:Config.messageMaxSize])
        return messages

    def getPowerInfo(self,mobilenum):
        messages =[]
        message = ''
        try:
            user_id=self.mongodb.users.find_one({'mobile':mobilenum},{'_id':1,'firstName':1})                      
            if user_id is not None:      
                message = message + user_id['firstName']                        
                records = self.mongodb.iotconfigs.find({"spouts.communities.farmers":{"$elemMatch":{"userId":str(user_id['_id'])}}})                                
                for rec in records:                                                                              
                    for node in rec['nodes']:                                                                                                
                        if node['type'] == 'MOTOR-CONTROL':                                
                            address =  node['permanentAddress']                                  
                            nodeSensorData = self.mongodb.iots.find({ 'data.nodePermanentAddress': str(address)})\
                                                        .sort([('createdAt',-1)])\
                                                        .limit(1)                                                                                    
                            for sd in nodeSensorData:                                                                                    
                                for sensordata in sd['data']['motor_data']: 
                                    if sensordata['type'] == 'ACTIVE-POWER':
                                        if sensordata['value'] == 0:
                                            message = message + ', Power Available'
                                        if sensordata['value'] == 14:
                                            message = message + ', Low Voltage'                                            
                                        if sensordata['value'] == 15:
                                            message = message + ', High Voltage'
                                        if sensordata['value'] == 22:
                                            message = message + ', No Power'                                    
                                    if sensordata['type'] == 'MOTOR-RUNNING-STATUS':        
                                        if sensordata['value']==40:
                                            message = message + ',MOTOR-OFF'
                                        if sensordata['value']==44:
                                            message = message + ',MOTOR-ON'                                            
            else:
                message = message + ' please do registration devcrm.kisanraja.com as farmer.'                                              
        except Exception as e:
            message = message + ',sorry unable to process you message.'
            error={'mobile':mobilenum,'type':'USERINFO','req-type':'SENSORINFO','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)                                   
        messages.append(message[0:Config.messageMaxSize])
        return messages
    def UserConfigbyNodeperAddr(self,nodePerAdd):
        UserConfig ={}
        try:            
            users = []                        
            wuarecords = self.mongodb.iotconfigs.find_one({"nodes.permanentAddress":nodePerAdd})
            if wuarecords is not None:                                
                UserConfig['WUAName'] =wuarecords['WUAName']
                UserConfig['WUAAdress']= wuarecords['WUAAdress']
                UserConfig['WUADescription']= wuarecords['WUADescription']
                UserConfig['scope']= 'All farmers'                
                users.append(wuarecords['WUAAdmin'])
                users.append(wuarecords['WUAOperator'])
                for spout in wuarecords['spouts']:      
                    for community in spout['communities']:                                      
                        for uId in community['farmers']:                        
                            users.append(str(uId['userId']))
            wuarecords = self.mongodb.iotconfigs.find_one({"spouts.nodes.permanentAddress":nodePerAdd})            
            if wuarecords is not None:                
                UserConfig['WUAName'] =wuarecords['WUAName']
                UserConfig['WUAAdress']= wuarecords['WUAAdress']
                UserConfig['WUADescription']= wuarecords['WUADescription']
                users.append(wuarecords['WUAAdmin'])
                users.append(wuarecords['WUAOperator'])
                for spout in wuarecords['spouts']:     
                    for community in spout['communities']: 
                        for node in community['nodes']:   
                            if node['permanentAddress'] == nodePerAdd:                               
                                UserConfig['scope']= community['name']                                      
                                for uId in community['farmers']:                                
                                    users.append(str(uId['userId']))         
                            
            subscribeusers= self.mongodb.iotnotificationconfigs.find({'userId':{'$in':users}})            
            UserConfig['users']=[]
            if subscribeusers is not None:
                for notificationconfig in subscribeusers:
                    if notificationconfig['sensorData'] == True or notificationconfig['hardwareData'] == True:
                        UserConfig['users'].append(notificationconfig['userId'])                                    
            return UserConfig                  
            
        except Exception as e:                                          
            error={'permanentAddress':nodePerAdd,'type':'SENSORINFO','req-type':'ALERT-NOTIFICATION','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)   
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)    
        return UserConfig    
   
   

class IotConfig:   
    mongoclient=MongoClient(Config.mongourl)
    mongodb=mongoclient[Config.mongodb]    
    def IsActivated(self,gatewayID):    
        flag =False 
        try:
            wuarecords = self.mongodb.iotconfigs.find_one({"isActive":True,"nodes.gatewaySerialNo":gatewayID})                
            if wuarecords is not None:
                return True
            wuarecords = self.mongodb.iotconfigs.find_one({"isActive":True,"spouts.nodes.gatewaySerialNo":gatewayID})                
            if wuarecords is not None:
                return True            
        except Exception as e:
            error={'gatewaySerialNo':nodePerAdd,'type':'IsActivated','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error) 
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                                  
        return flag
class IrrigationSchedule(object):
    mongoclient=MongoClient(Config.mongourl)
    mongodb=mongoclient[Config.mongodb]
    def getSchedule(self):
        schedules =[]
        try:
            today =  datetime.now()        
            today=today.replace(hour=0, minute=0, second=0, microsecond=0)      
            nextday =  today + timedelta(days=1)          
            irrsches = self.mongodb.irrigationschedules.find({'sowingStartDate':{'$lte':today},'sowingEndDate':{'$gte':today}})         
            for item in irrsches:            
                schedule={}
                schedule['wuaId']=item['wuaId']
                schedule['sensorName']=item['sensorName']
                schedule['spoutId']=item['spoutId']    
                schedule['communityId']= item['communityId']      
                for day in item['schedule']:
                    if day['day'] >= today and day['day'] < nextday:
                        schedule['day']= today                
                        schedule['maxValue']=float(day['maxValue'])
                        schedule['minValue']=float(day['minValue'])
                        schedule['daycount']=int(day['daycount'])
                        schedules.append(schedule)    
        except Exception as e:
            error={'class':'IrrigationSchedule','module':'getSchedule','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)      
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                                              
        return schedules
    def getSensorAndFarmerData(self,schedules):
        scheduleSensorFarmerDataPoints = []
        try:
            for schedule in schedules:    
                scheduleSensorFarmerDataPoint = schedule           
                wuarecords = self.mongodb.iotconfigs.find_one({'_id':ObjectId(schedule['wuaId'])},{'WUAName':1,'spouts':1})                
                for spout in wuarecords['spouts']:
                    for community in spout['communities']:                    
                        if ObjectId(community['_id']) == ObjectId(schedule['communityId']):
                            scheduleSensorFarmerDataPoint['wuaName']=wuarecords['WUAName']
                            scheduleSensorFarmerDataPoint['spoutName']=spout['name']
                            scheduleSensorFarmerDataPoint['communityName']=community['name']
                            scheduleSensorFarmerDataPoint['farmers'] = []                            
                            for farmer in community['farmers']:                                
                                print (farmer)
                                scheduleSensorFarmerDataPoint['farmers'].append(farmer['userId'])                        
                            for node in community['nodes']:
                                for sensor in  node['sensors']:                                
                                    if sensor['name']==schedule['sensorName']:
                                        scheduleSensorFarmerDataPoint['permanentAddress']=node['permanentAddress']
                                        nodeSensorData = self.mongodb.iots.find({ 'data.nodePermanentAddress': str(node['permanentAddress'])})\
                                                                .sort([('createdAt',-1)])\
                                                                .limit(1)                                                    
                                        for sd in nodeSensorData:                                        
                                            for sensordata in sd['data']['sensors']:                                            
                                                if sensordata['id'] == schedule['sensorName']:                           
                                                    addons={'consc':1,'consk':1,'conswet':1,'conssoilmax':1}                                                                                                                                                                       
                                                    if sensordata['id']== 'water-level':                                                                    
                                                        for addson in sensor['addOns']:                                                
                                                            if addson['field']=='Consc':
                                                                addons['consc']= addson['value']  
                                                            if addson['field']=='Consk':
                                                                addons['consk']= addson['value']  
                                                    if sensordata['id']== 'soil-moisture':                                            
                                                        for addson in sensor['addOns']:                                                
                                                            if addson['field']=='conswet':
                                                                addons['conswet']= addson['value']  
                                                            if addson['field']=='consmax':
                                                                addons['conssoilmax']= addson['value']                                                                                
                                                    scheduleSensorFarmerDataPoint['sensorData']=apiutils.calibrate(schedule['sensorName'],sensordata['value'],addons)                                    
                scheduleSensorFarmerDataPoints.append(scheduleSensorFarmerDataPoint)                            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)    
            error={'class':'IrrigationSchedule','module':'getSensorAndFarmerData','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)     
        return scheduleSensorFarmerDataPoints
    def getMotorData(self):            
            try:
                motordatapoints ={}
                for wuaid in self.mongodb.iotconfigs.find({},{'nodes':1,'WUAOperator':1}):                                                                         
                    motordatapoints[str(wuaid['_id'])]={}
                    for node in wuaid['nodes']:                                   
                        motordatapoints[str(wuaid['_id'])]['WUAOperator']= wuaid['WUAOperator']                                                                                
                        if node['type'] == 'MOTOR-CONTROL':    
                            motordatapoints[str(wuaid['_id'])]['MOTOR']='OFF'
                            for addon in node['sensors'][0]['addOns']:
                                if addon['field']=='simultaneous-spouts':
                                    motordatapoints[str(wuaid['_id'])]['simultaneous-spouts']=int(addon['value'])
                            motordatapoints[str(wuaid['_id'])]['gateWaySerialNo'] = node ['gatewaySerialNo']           
                            nodeSensorData = self.mongodb.iots.find({ 'data.nodePermanentAddress': str(node['permanentAddress'])})\
                                                        .sort([('createdAt',-1)])\
                                                        .limit(1)                                                                                    
                            for sd in nodeSensorData:                                                                                    
                                for sensordata in sd['data']['motor_data']:                                                                    
                                    if sensordata['type'] == 'MOTOR-RUNNING-STATUS':        
                                        if sensordata['value']==44:
                                            motordatapoints[str(wuaid['_id'])]['MOTOR']='ON'                                                                                                                                                    
            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(exc_type, fname, exc_tb.tb_lineno)                                    
                error={'class':'IrrigationSchedule','module':'getMotorData','error':e.message,'timestamp':datetime.now()}
                self.mongodb.ioterrors.insert(error)                           
            return motordatapoints

    def getIrrigationAlert(self,mode,interval):
        try:   
            alerts ={}                              
            today =  datetime.now()- timedelta(minutes=interval*60)                      
            for sendalerts in self.mongodb.iotalerts.find({'function-type':'irrsche','notification-mode':mode,'send-time':{'$gte':today}}):                                                                                                  
                alerts[sendalerts['unitId']]={}
                alerts[sendalerts['unitId']][sendalerts['alert-type']]=True    
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                                    
            error={'class':'IrrigationSchedule','module':'getIrrigationAlert','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)                           
        return alerts    
    def getUserDetails(self,users):
        try:
            userDatapoints ={}                                                       
            for userInfo in self.mongodb.users.find({'_id':{'$in':users}},{'_id':1,'mobile':1,'firstName':1}):                                                                                                                
                userDatapoints[str(userInfo['_id'])]={}
                userDatapoints[str(userInfo['_id'])]['mobile']=userInfo['mobile']                       
                userDatapoints[str(userInfo['_id'])]['name']=userInfo['firstName']                                                                                                                                                      
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                                    
            error={'class':'IrrigationSchedule','module':'getIrrigationAlert','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)                           
        return userDatapoints             
class Alerts(object):    
    mongoclient=MongoClient(Config.mongourl)
    mongodb=mongoclient[Config.mongodb]
    def insertAlert(self,input):
        try:            
            self.mongodb.iotalerts.insert(input)
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)                                    
            error={'class':'IrrigationSchedule','module':'getIrrigationAlert','error':e.message,'timestamp':datetime.now()}
            self.mongodb.ioterrors.insert(error)                   