'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
''' Kisan Raja IOT Sensor API '''
import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
from datetime import datetime
import logging
import json
import sys
import os
import apiutils
import apistatic
import datainterface

class Config(object):
    mongourl ='localhost:27017'
    mongodb='kisanraja-dev-db'

    iotchannel ='iot.kisanraja.com'
    iotport = 1883
    iotkeepalive=60

    logname= 'apisensor.log'

class Static(object):
    DSENSOR_ID = {
        9:'temparature-frequent',
        120:'node-operating-voltage',               
        33:'pressure',
        36:'water-level',
        44:'diver-pressure',
        45:'diver-temperature',
        46:'diver-mod-pressure',
        47:'diver-mod-temperature',
        52:'pulse-count-water-meter',
        53:'pulse-count-rain-gauge',
        104:'node-battary-voltage',
        145:'Moisture_CHIRP',
        147:'soil-moisture',
        176:'humidity',
        177:'temparature-stable'}
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("sensor/#")

def getMessageType(msg_type_msb,msg_type_lsb):
    messagetype = int(msg_type_msb + msg_type_lsb,16)
    if messagetype == 6:
        return 'RELAY_DATA_FROM_NODE'
    elif messagetype == 176:
        return 'RELAY_EVENT_FROM_NODE'    
    else:
        return 'UNDEF'
def getPayloadLength(payload_length_msb,payload_length_lsb):
        return  int(payload_length_msb + payload_length_lsb,16)

def getHeaderCRC(header_crc_msb,header_crc_lsb):
    return  int(header_crc_msb + header_crc_lsb,16)

def getPayloadCRC(payload_crc_msb,payload_crc_lsb):
    return  int(payload_crc_msb + payload_crc_lsb,16)
def getNodeShortAddres(node_short_addr_msb,node_short_addr_lsb):
    return  int(node_short_addr_msb + node_short_addr_lsb,16)
def getPayloadType(payloadtypeoffset):
    if payloadtypeoffset == 3:
        return 'SENSOT_OUT_PUT'
    else:
        return 'UNDEF'    
def getTLVType(tlvtypeflag):
    if tlvtypeflag == 16:
        return 'SENSOT_LIST'    
    else:
        return 'UNDEF'        
def getTLVSensorOutCodes(dataflag):
    if dataflag == 20:
        return 'SENSOR_ID'
    elif dataflag == 2:    
        return 'SENSOR_DATA'
    elif dataflag == 4:    
        return 'SENSOR_SCALE_FACTOR'
    else:
        return 'UNDEF'        
def getEventByID(evetnid):
        id=int(evetnid,16)
        if id ==1:
            return 'NODE_REG'
        else:
            return 'UNDEF'        
def getControllerFlag(cntlflag):
    flag = int(cntlflag,16)
    if flag == 77:
        return 'MOTOR-CONTROLLER'
    elif flag == 83:
        return 'SENSOR-CONTROLLER'
    elif flag == 71:
        return 'GATE-WAY'
    else:
        return 'UNDEF'


def getMotorControllerFlags(flagvalue): 
    if flagvalue == 0:
        return 'PRE-VOLTAGE-RY'
    elif flagvalue == 1:
        return 'PRE-VOLTAGE-YB'    
    elif flagvalue == 2:
        return 'PRE-VOLTAGE-BR'      
    elif flagvalue == 3:
        return 'VOLTAGE-RY'
    elif flagvalue == 4:
        return 'VOLTAGE-YB'    
    elif flagvalue == 5:
        return 'VOLTAGE-BR'
    elif flagvalue == 6:
        return 'PRE-CURRENT-R'  
    elif flagvalue == 7:
        return 'PRE-CURRENT-Y'  
    elif flagvalue == 8:
        return 'PRE-CURRENT-B'      
    elif flagvalue == 9:
        return 'CURRENT-R'  
    elif flagvalue == 10:
        return 'CURRENT-Y'  
    elif flagvalue == 11:
        return 'CURRENT-B'  
    elif flagvalue == 12:
        return 'REF-CURRENT-R'  
    elif flagvalue == 13:
        return 'REF-CURRENT-Y'  
    elif flagvalue == 14:
        return 'REF-CURRENT-B'      
    elif flagvalue == 15:
        return 'SMPS'  
    elif flagvalue == 16:
        return 'BATTARY'  
    elif flagvalue == 17:
        return 'ACTIVE-POWER'  
    elif flagvalue == 18:
        return 'HARDWARE-ERROR'  
    elif flagvalue == 19:
        return 'CURRENT-PHAGE'      
    elif flagvalue == 20:
        return 'MOTOR-RUNNING-STATUS'
    elif flagvalue == 21:
        return 'EEPROM-LOC' 
    elif flagvalue == 22 :
        return 'C-RANGE'           
    elif flagvalue == 23 :
        return 'EEPROM-PRE-VAL'
    elif flagvalue == 24 :
        return 'EEPROM-NEW-VAL' 
    elif flagvalue == 25 :
        return 'CALLER-NO'
    elif flagvalue == 26 :
        return 'PRE-CALLER-NO'
    elif flagvalue == 27 :
        return 'NEW-CALLER-NO'                   
    elif flagvalue == 127:
        return 'FARMERMOBILE'  
    else:
        return 'UNDEF'        
def getMotorDataSizeByFlag(motordataflag):    
    if  motordataflag =='ACTIVE-POWER' \
        or motordataflag =='HARDWARE-ERROR' \
        or motordataflag =='CURRENT-PHAGE' \
        or motordataflag =='MOTOR-RUNNING-STATUS' \
        :
            return 1
    elif motordataflag =='VOLTAGE-RY' \
        or motordataflag =='VOLTAGE-YB' \
        or motordataflag =='VOLTAGE-BR' \
        or motordataflag =='PRE-VOLTAGE-RY' \
        or motordataflag =='PRE-VOLTAGE-YB' \
        or motordataflag =='PRE-VOLTAGE-BR' \
        or motordataflag =='PRE-CURRENT-R' \
        or motordataflag =='PRE-CURRENT-Y' \
        or motordataflag =='PRE-CURRENT-B' \
        or motordataflag =='REF-CURRENT-R' \
        or motordataflag =='REF-CURRENT-Y' \
        or motordataflag =='REF-CURRENT-B' \
        or motordataflag =='CURRENT-R' \
        or motordataflag =='CURRENT-Y' \
        or motordataflag =='CURRENT-B' \
        or motordataflag =='SMPS' \
        or motordataflag =='BATTARY' \
        or motordataflag =='C-RANGE'\
        or motordataflag =='EEPROM-LOC'\
        or motordataflag =='EEPROM-NEW-VAL'\
        or motordataflag =='EEPROM-PRE-VAL'\
        :
            return 2    
    elif motordataflag=='CALLER-NO'\
         or motordataflag =='FARMERMOBILE'\
         or motordataflag == 'NEW-CALLER-NO'\
         or motordataflag =='PRE-CALLER-NO'\
        :
            return 11
    else :
        raise Exception('data type size is not defined %s',motordataflag)
def getMotorDataunits(motordataflag):
    if motordataflag =='VOLTAGE-RY' \
        or motordataflag =='VOLTAGE-YB' \
        or motordataflag =='VOLTAGE-BR' \
        or motordataflag =='PRE-VOLTAGE-RY' \
        or motordataflag =='PRE-VOLTAGE-YB' \
        or motordataflag =='PRE-VOLTAGE-BR' \
        or motordataflag =='SMPS'\
        or motordataflag =='BATTARY'\
        :
        return 'Volts'
    elif motordataflag =='CURRENT-R' \
        or motordataflag =='CURRENT-Y' \
        or motordataflag =='CURRENT-B' \
        or motordataflag =='PRE-CURRENT-R' \
        or motordataflag =='PRE-CURRENT-Y' \
        or motordataflag =='PRE-CURRENT-B' \
        or motordataflag =='REF-CURRENT-R' \
        or motordataflag =='REF-CURRENT-Y' \
        or motordataflag =='REF-CURRENT-B' \
        :
        return 'watts'
    elif  motordataflag =='ACTIVE-POWER' \
        or motordataflag =='HARDWARE-ERROR' \
        or motordataflag =='CURRENT-PHAGE' \
        or motordataflag =='MOTOR-RUNNING-STATUS' \
        :
        return 'info'
    elif motordataflag =='CALLER-NO'\
        or motordataflag =='FARMERMOBILE'\
        or motordataflag =='NEW-CALLER-NO'\
        or motordataflag =='PRE-CALLER-NO'\
        :
            return 'mobile'
    elif motordataflag ==' C-RANGE'\
        or motordataflag =='EEPROM-LOC'\
        or motordataflag =='EEPROM-NEW-VAL'\
        or motordataflag =='EEPROM-PRE-VAL'\
        :
        return 'RELOOK'
    else :
        return 'N/A'

def processUserInfomessage(client,gatewayId,datatype,mobileno):
    processrecord = {}
    processrecord ['sensor-type']  ='M'       
    processrecord ['communication-type']  ='USERINFO'       
    processrecord ['data-type'] =datatype
    processrecord ['farmer-mobile'] = mobileno
    processrecord ['gatewaySerialNo'] = gatewayId    
    client.publish(apistatic.PROCESSTOPIC+gatewayId,str(processrecord),1,retain=False)
def sendnotification(client,commtype,nodePermanentAddress,payload):
    processrecord = {}
    processrecord ['communication-type']  =commtype
    processrecord ['nodePermanentAddress'] = nodePermanentAddress
    processrecord.update(payload)
    client.publish(apistatic.PROCESSTOPIC+nodePermanentAddress,str(processrecord),1,retain=False)
def on_message(client, userdata, message):       
    logging.debug('topic : %s',message.topic)          
    if 'sensor/' in message.topic:        
        try:
            iotrecod = {}
            isactiveFlag = diIotConfig.IsActivated(message.topic[7:])
            if isactiveFlag == True:                
                iotrecod['topic']=message.topic            
                iotrecod['createdAt']=datetime.now()
                dict_message = { }                   
                messagebytes = [ ]                          
                for ele in message.payload:                
                    messagebytes.append(ele.encode('hex'))    
                iotrecod['message']=messagebytes
                flag = getControllerFlag(messagebytes[0])
                messagebytes= messagebytes[1:]            
                dict_message['CONTROLLER']=flag
                logging.debug('flag : %s',flag)                
                if flag =='MOTOR-CONTROLLER':                                                          
                    dict_message['motor-id']=  int(messagebytes[0],16)
                    dict_message['request-type'] = apistatic.getMotorControllerRequestType(messagebytes[1])
                    dict_message['nodeShortAddress'] = dict_message['motor-id']
                    dict_message['nodePermanentAddress'] = 'M'+str(dict_message['motor-id'])+message.topic[7:]                         
                    if dict_message['request-type'] == 'STATUS':
                        dict_message['motor-info-data-type'] = apistatic.GetMotorInfoDataType(int(messagebytes[2],16))                                        
                        dict_message['payload-size']  = apiutils.getnumber(messagebytes[4],messagebytes[3])                    
                        payload = messagebytes[5:]           
                        dataobject='NA' 
                        if dict_message['motor-info-data-type'] == 'REGULARUPDATE'\
                            or dict_message['motor-info-data-type'] == 'ERRORUPDATE'\
                            :\
                            dataobject ='motor_data'          
                        elif dict_message['motor-info-data-type'] == 'EPPROMUPDATE':
                            dataobject ='eeprom-data'
                        else:
                            iotrecod['unknown']='unknown motor controller motor-info-data-type'
                            logging.debug('unknow Record :%s',str(iotrecod)) 
                            mongocollectioniotuknown.insert(iotrecod)                    
                        if dataobject != 'NA':
                            dict_message[dataobject]= []
                            cnt  =0 
                            dataflagvalue =0
                            while cnt<dict_message['payload-size']:              
                                dataflagvalue = apiutils.skipMotordataflags(dict_message['motor-info-data-type'],dataflagvalue)                                          
                                dataflag = getMotorControllerFlags(dataflagvalue)                             
                                if getMotorDataSizeByFlag(dataflag) == 11:
                                    dataitem = {'type':dataflag,'value':apiutils.getmobileNumber(payload[cnt:11]),'units':getMotorDataunits(dataflag)}   
                                if getMotorDataSizeByFlag(dataflag) == 2:
                                    dataitem = {'type':dataflag,'value':apiutils.getnumber(payload[cnt+1],payload[cnt]),'units':getMotorDataunits(dataflag)}   
                                    cnt = cnt + 2      
                                elif getMotorDataSizeByFlag(dataflag) == 1:    
                                    dataitem = {'type':dataflag,'value':apiutils.bytetonum(payload[cnt]),'units':getMotorDataunits(dataflag)}   
                                    cnt = cnt + 1    
                                else:
                                    raise Exception('data size  mapping required %s',dataflag)                            
                                dataflagvalue = dataflagvalue + 1
                                dict_message[dataobject].append(dataitem)        
                            iotrecod['data'] =dict_message        
                            logging.debug('motor controller /status/final Record :%s',str(iotrecod))     
                            if dict_message['motor-info-data-type'] == 'REGULARUPDATE'\
                                or dict_message['motor-info-data-type'] == 'ERRORUPDATE'\
                                :
                                mongocollectioniot.insert(iotrecod)
                            else:
                                mongocollectioniotinfo.insert(iotrecod)                                          
                            sendnotification(client,'NOTIFICATION-MOTOR-STATUS',dict_message['nodePermanentAddress'],{'motor_data':dict_message[dataobject]});
                    elif dict_message['request-type'] == 'USERINFO':                       
                        dict_message['user-info-data-type'] = apistatic.GetMotorInfoDataType(int(messagebytes[2],16))
                        if dict_message['user-info-data-type'] =='SENSORINFO' or \
                                dict_message['user-info-data-type'] =='IRRSCHEINFO'or \
                                dict_message['user-info-data-type'] =='POWERINFO':
                            dict_message['payload-size']  = apiutils.getnumber(messagebytes[4],messagebytes[3])                         
                            dict_message['farmer-mobile'] = apiutils.getmobileNumber(messagebytes[5:])
                            iotrecod['data'] =dict_message        
                            logging.debug('motor controller /userinfo/final Record :%s',str(iotrecod))                                     
                            mongocollectioniotuser.insert(iotrecod)
                            processUserInfomessage(client,message.topic[7:],dict_message['user-info-data-type'],dict_message['farmer-mobile'])
                        else:
                            iotrecod['unknown']='unknown motor controller user-info-data-type'
                            logging.debug('unknow Record :%s',str(iotrecod)) 
                            mongocollectioniotuknown.insert(iottrecod)                                                              
                    else:
                        iotrecod['unknown']='unknown motor controller request type flags'
                        logging.debug('unknow Record :%s',str(iotrecod))                                     
                        mongocollectioniotuknown.insert(iottrecod)                                             
                elif flag =='SENSOR-CONTROLLER':
                    dict_message['CONTROLLER']='SENSOR-CONTROLLER'                
                    dict_message['messageType']=getMessageType(messagebytes[0],messagebytes[1])
                    if dict_message['messageType'] == 'RELAY_DATA_FROM_NODE':
                        dict_message['messageFlags'] = int(messagebytes[2],16)
                        dict_message['messageSeqNumber'] = int(messagebytes[3],16)            
                        dict_message['messagePayloadLength']=getPayloadLength(messagebytes[4],messagebytes[5])
                        dict_message['messageHeaderCrc']=getHeaderCRC(messagebytes[6],messagebytes[7])
                        dict_message['messagePayloadCrc']=getPayloadCRC(messagebytes[8],messagebytes[9])
                        payload = messagebytes[10:dict_message['messagePayloadLength']+10]
                        dict_message['nodeShortAddress'] = getNodeShortAddres(payload[0],payload[1])
                        dict_message['nodePermanentAddress'] = ':'.join(payload[2:10])
                        dict_message['channelRSSI'] =int(payload[10],16)
                        dict_message['channelLQI']=int(payload[11],16)
                        dict_message['payloadType'] = getPayloadType(int(payload[12],16))
                        if dict_message['payloadType'] == 'SENSOT_OUT_PUT':
                            dict_message['tlvType'] = getTLVType(int(payload[13],16))
                            dict_message['tlvSize'] = int(payload[14],16)
                            tlv = payload[15:dict_message['tlvSize']+15]
                            tlvcounter = 0
                            logging.debug('tlv :%s',tlv)
                            while True:
                                dict_sensor =                                                      
                                if int(tlv[tlvcounter],16) == 17:                                 
                                    tlvcounter  = tlvcounter +1
                                    tlvobjectsize= int(tlv[tlvcounter],16)
                                    tlvcounter  = tlvcounter +1
                                    tlvobject = tlv[tlvcounter:tlvcounter+tlvobjectsize]
                                    tlvobjectcounter=0           
                                    logging.debug('tlvobject :%s',tlvobject)             
                                    while tlvobjectsize>tlvobjectcounter :
                                        if getTLVSensorOutCodes(int(tlvobject[tlvobjectcounter],16)) == 'SENSOR_ID':
                                            tlvobjectcounter = tlvobjectcounter +1
                                            sensoridsize = int(tlvobject[tlvobjectcounter],16)
                                            tlvobjectcounter = tlvobjectcounter +1
                                            sensorID =''
                                            while sensoridsize>0:
                                                sensorID = sensorID + tlvobject[tlvobjectcounter]
                                                tlvobjectcounter = tlvobjectcounter +1
                                                sensoridsize = sensoridsize -1                       
                                            logging.debug('sensor raw id :%d',int(sensorID,16))                     
                                            if int(sensorID,16) not in Static.DSENSOR_ID:
                                                logging.error('sensor mapping required %d'%int(sensorID,16))
                                                raise Exception('sensor mapping required %d'%int(sensorID,16))
                                            dict_sensor['id']=Static.DSENSOR_ID[int(sensorID,16)]   
                                        elif getTLVSensorOutCodes(int(tlvobject[tlvobjectcounter],16)) == 'SENSOR_DATA':
                                            tlvobjectcounter = tlvobjectcounter +1
                                            sensordatasize = int(tlvobject[tlvobjectcounter],16)
                                            tlvobjectcounter = tlvobjectcounter +1
                                            sensordata =''
                                            while sensordatasize>0:
                                                sensordata = sensordata + tlvobject[tlvobjectcounter]
                                                tlvobjectcounter = tlvobjectcounter +1
                                                sensordatasize = sensordatasize - 1
                                            dict_sensor['value']=int(sensordata,16)
                                        elif getTLVSensorOutCodes(int(tlvobject[tlvobjectcounter],16)) == 'SENSOR_SCALE_FACTOR':
                                            tlvobjectcounter = tlvobjectcounter +1
                                            sensorscalesize = int(tlvobject[tlvobjectcounter],16)
                                            tlvobjectcounter = tlvobjectcounter +1
                                            sensorscale =''
                                            while sensorscalesize>0:
                                                sensorscale = sensorscale + tlvobject[tlvobjectcounter]
                                                tlvobjectcounter = tlvobjectcounter +1
                                                sensorscalesize = sensorscalesize -1
                                            dict_sensor['scale']=int(sensorscale,16)  
                                        else:
                                            raise Exception('incorret format of message,tlv has undefined format incorrect')
                                    tlvcounter = tlvcounter +   tlvobjectcounter      
                                    if 'sensors' not in dict_message:
                                        dict_message['sensors']=[]
                                    dict_message['sensors'].append(dict_sensor)                                                   
                                else:
                                    raise Exception('incorret format of message,tlv 0 byte incorrect')                        
                                if tlvcounter >= len(tlv):
                                    break                    
                        else:
                            iotrecod['unknown']='unknown message byte 12(13)'
                            logging.debug('unknow Record :%s',str(iotrecod))                                     
                            mongocollectioniotuknown.insert(iotrecod)
                        iotrecod['data']=dict_message     
                        logging.debug('final Record :%s',str(iotrecod)) 
                        mongocollectioniot.insert(iotrecod)                                                    
                        mongocollectioniotconfig.update({"nodes.permanentAddress":dict_message['nodePermanentAddress']},{"$set":{"nodes.$.shortAddress":dict_message['nodeShortAddress']}})                                   
                        result = mongocollectioniotconfig.find_one({"spouts.communities.nodes.permanentAddress":dict_message['nodePermanentAddress']})                    
                        cntspout = -1
                        if result is not None:
                            for spout in result['spouts']:
                                cntspout = cntspout + 1
                                cntcommunity = -1
                                for community in spout['communities']:
                                    cntcommunity = cntcommunity + 1
                                    cntnode = -1                                    
                                    for node in community['nodes']:
                                        cntnode = cntnode +1
                                        if node['permanentAddress'] == dict_message['nodePermanentAddress']:                                  
                                            mongocollectioniotconfig.find_one_and_update({"spouts.communities.nodes.permanentAddress":dict_message['nodePermanentAddress']},{ "$set": {"spouts."+str(cntspout)+".communities."+str(cntcommunity)+".nodes."+str(cntnode)+".shortAddress":dict_message['nodeShortAddress']}})                                 
                    elif dict_message['messageType'] == 'RELAY_EVENT_FROM_NODE':                    
                        if getEventByID(messagebytes[10]) == 'NODE_REG':
                            dict_message['payloadType'] ='NODE_REG'
                            shortaddr=getNodeShortAddres(messagebytes[11],messagebytes[12])                        
                            permantaddr =':'.join(messagebytes[13:21])                        
                            mongocollectioniotconfig.update({"nodes.permanentAddress":permantaddr},{ "$set": {"nodes.$.shortAddress":shortaddr}})                    
                            result = mongocollectioniotconfig.find_one({"spouts.communities.nodes.permanentAddress":permantaddr})                    
                            cntspout = -1
                            if result is not None:
                                for spout in result['spouts']:
                                    cntspout = cntspout + 1
                                    cntcommunity = -1
                                    for community in spout['communities']:
                                        cntcommunity = cntcommunity + 1
                                        cntnode = -1                                    
                                        for node in community['nodes']:
                                            cntnode = cntnode +1
                                            if node['permanentAddress'] == permantaddr:                        
                                                mongocollectioniotconfig.find_one_and_update({"spouts.communities.nodes.permanentAddress":permantaddr},{ "$set": {"spouts."+str(cntspout)+".communities."+str(cntcommunity)+".nodes."+str(cntnode)+".shortAddress":shortaddr}})                    
                            dict_message['nodeShortAddress'] =shortaddr
                            dict_message['nodePermanentAddress'] =permantaddr
                            sendnotification(client,'NOTIFICATION-SENSOR-EVENT',permantaddr,dict_message);                                        
                        else:
                            iotrecod['unknown']='unknown event message byte 10(11)'
                            logging.debug('unknow Record :%s',str(iotrecod))                                     
                            mongocollectioniotuknown.insert(iotrecod)                                             
                    else:                      
                        iotrecod['unknown']='unknown message byte 0,1'
                        logging.debug('unknow Record :%s',str(iotrecod)) 
                        mongocollectioniotuknown.insert(iotrecod)   
                else:
                    iotrecod['unknown']='unknown controller flags'
                    logging.debug('unknow Record :%s',str(iotrecod)) 
                    mongocollectioniotuknown.insert(iotrecod)                                                 
            else:
                mongocollectioniotinactive.update({'gatewaySerialNo':message.topic[7:]},{'gatewaySerialNo':message.topic[7:],'createAt' : datetime.now()},upsert=True)
        except Exception as e:             
            iotrecod['error']=e.message                     
            print iotrecod
            print e
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            logging.error(str(e.message)+'%s',str(iotrecod))            
            mongocollectionioterror.insert(iotrecod) 

logging.basicConfig(filename=Config.logname,level=logging.WARNING)
diIotConfig = datainterface.IotConfig()
mqttclient = mqtt.Client()
mqttclient.on_connect = on_connect
mqttclient.on_message = on_message
mongoclient=MongoClient(Config.mongourl)
mongodb=mongoclient[Config.mongodb]
mongocollectioniot=mongodb.iots
mongocollectioniotinfo=mongodb.iotinfos
mongocollectionioterror=mongodb.ioterrors
mongocollectioniotuknown=mongodb.iotunknowns
mongocollectioniotuser=mongodb.iotusers
mongocollectioniotconfig=mongodb.iotconfigs
mongocollectioniotinactive=mongodb.iotinactives
mqttclient.connect(Config.iotchannel, Config.iotport, Config.iotkeepalive)
mqttclient.loop_forever()

