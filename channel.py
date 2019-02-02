'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
import sys
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import time
from random import randint
import json
import krcrc16

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

client = mqtt.Client()
client.on_connect = on_connect
client.connect("iot.kisanraja.com", 1883, 60)
def getgetChannel(gatewayId):    
    sensormessage= '00c800000002ff35fff5000a'    
    sensormessage = sensormessage.decode('hex')                                          
    client.publish("cloud/"+gatewayId,sensormessage,1,retain=False)
    print 'seng messaga to '+gatewayId
def setChannel(gatewayId,channelNo):
    sensormessage= '{0:02x}'.format(ord('S'))
    sensorpayload = '000a'+'{0:02x}'.format(int(channelNo))
    crc16 =krcrc16.crc16(sensorpayload,len(sensorpayload)/2)
    sensorpayloadcrc = '{:04x}'.format(crc16)[-4:]
    header ='00c900000003ff33'+sensorpayloadcrc
    sensormessage= sensormessage + header+ sensorpayload               
    sensormessage = sensormessage.decode('hex')                                          
    client.publish("cloud/"+gatewayId,sensormessage,1,retain=False)
    print 'seng messaga to %s with channe No :%s'%(gatewayId,channelNo)
def cleatWhiteList(gatewayId):    
    sensormessage= '{0:02x}'.format(ord('S'))
    sensormessage= sensormessage+'004500000000ffba0000'    
    sensormessage = sensormessage.decode('hex')                                          
    client.publish("cloud/"+gatewayId,sensormessage,1,retain=False)
    print 'seng messaga to '+gatewayId
def addtoWhiteList(gatewayId,mac):
    sensormessage= '{0:02x}'.format(ord('S'))
    sensorpayload =mac.replace(':','').strip()
    crc16 =krcrc16.crc16(sensorpayload,len(sensorpayload)/2)
    sensorpayloadcrc = '{:04x}'.format(crc16)[-4:]
    header ='004300000008ffb4'+sensorpayloadcrc
    sensormessage= sensormessage + header+ sensorpayload               
    sensormessage = sensormessage.decode('hex')                                              
    client.publish("cloud/"+gatewayId,sensormessage,1,retain=False)
    print 'seng messaga to %s with channe No :%s'%(gatewayId,mac)
def restartNode(gatewayId,shortaddress):
    sensormessage= '{0:02x}'.format(ord('S'))
    sensorpayload = '{0:04x}'.format(int(shortaddress)) +'0e'
    crc16 =krcrc16.crc16(sensorpayload,len(sensorpayload)/2)
    sensorpayloadcrc = '{:04x}'.format(crc16)[-4:]
    header ='000500000003fff7'+sensorpayloadcrc
    sensormessage= sensormessage + header+ sensorpayload               
    sensormessage = sensormessage.decode('hex')                                          
    client.publish("cloud/"+gatewayId,sensormessage,1,retain=False)
    print 'seng messaga to %s with channe No :%s'%(gatewayId,shortaddress)
if len(sys.argv)>1:        
    try:
        cmd = sys.argv[1]
        if cmd =='sc':
            gatewayId = sys.argv[2]
            channelNo = sys.argv[3]
            setChannel(gatewayId,channelNo)
        elif cmd =='wl-clr':
            gatewayId = sys.argv[2]
            cleatWhiteList(gatewayId)    
        elif cmd =='wl-add':    
            gatewayId = sys.argv[2]    
            mac = sys.argv[3]
            addtoWhiteList(gatewayId,mac)
        elif cmd =='rstn':    
            gatewayId = sys.argv[2]    
            shortAddress = sys.argv[3]    
            restartNode(gatewayId,shortAddress)
        elif cmd =='gc':
            gatewayId = sys.argv[2]   
            getgetChannel(gatewayId)
        else:
            print 'Usage is : 1(get channel) -> python channel.pyc gc <<gateway serial No.>> \n\
                                            Ex : python channel.pyc gc SAM0405 \n\
                            2(set channel) -> python channel.pyc sc <<gateway serial No.>> <<channel No>> \n\
                                            Ex :python channel.pyc sc SAM0405 1 \n\
                            3(clear white list) -> python channel.pyc wl-clr <<gateway serial No.>>  \n\
                                            Ex -> python channel.pyc wl-clr SAM0405 \n\
                            4(add to white list) -> python channel.pyc wl-add <<gateway serial No.>> <<mac/PermanentAddress>>  \n\
                                            Ex -> python channel.pyc wl-add SAM0405 fc:c2:3d:00:00:10:b2:f7 \n\
                            5(restart node) -> python channel.pyc rstn <<gateway serial No.>> <<shortAddress>>  \n\
                                            Ex -> python channel.pyc rstn SAM0405 1682 \n\
                        '
    except Exception as e:
            print e
            print 'Usage is : 1(get channel) -> python channel.pyc gc <<gateway serial No.>> \n\
                                            Ex : python channel.pyc gc SAM0405 \n\
                            2(set channel) -> python channel.pyc sc <<gateway serial No.>> <<channel No>> \n\
                                            Ex :python channel.pyc sc SAM0405 1 \n\
                            3(clear white list) -> python channel.pyc wl-clr <<gateway serial No.>>  \n\
                                            Ex -> python channel.pyc wl-clr SAM0405 \n\
                            4(add to white list) -> python channel.pyc wl-add <<gateway serial No.>> <<mac/PermanentAddress>>  \n\
                                            Ex -> python channel.pyc wl-add SAM0405 fc:c2:3d:00:00:10:b2:f7 \n\
                            5(restart node) -> python channel.pyc rstn <<gateway serial No.>> <<shortAddress>>  \n\
                                            Ex -> python channel.pyc rstn SAM0405 1682 \n\
                        '                                
else:
    print 'Usage is : 1(get channel) -> python channel.pyc gc <<gateway serial No.>> \n\
                                    Ex : python channel.pyc gc SAM0405 \n\
                    2(set channel) -> python channel.pyc sc <<gateway serial No.>> <<channel No>> \n\
                                    Ex :python channel.pyc sc SAM0405 1 \n\
                    3(clear white list) -> python channel.pyc wl-clr <<gateway serial No.>>  \n\
                                    Ex -> python channel.pyc wl-clr SAM0405 \n\
                    4(add to white list) -> python channel.pyc wl-add <<gateway serial No.>> <<mac/PermanentAddress>>  \n\
                                    Ex -> python channel.pyc wl-add SAM0405 fc:c2:3d:00:00:10:b2:f7 \n\
                    5(restart node) -> python channel.pyc rstn <<gateway serial No.>> <<shortAddress>>  \n\
                                    Ex -> python channel.pyc rstn SAM0405 1682 \n\
                  '                      