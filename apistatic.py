'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
PROCESSTOPIC = 'processing/eepaudadl/'
SMSTYPE ='USERINFOUNIOCDE'
#LANGUAGE ='TELUGU'
#LANGUAGE ='BENGAL'
LANGUAGE ='HINDI'

#SMSTYPE ='USERINFO'


SMSWAITTIME = 30

class ALRTTYPE(object):
         STARTMOTOR=0
         STOPMOTOR=1
         STARTIRRIGATION=2
         EXCESSWATER=3 
         SUFFICIENTWATER=4          
         LOWBATTRY = 5             
         BATTRYFULL = 6   

def getMotorControllerRequestType(requestflag):
    flagvalue = int(requestflag,16)
    if flagvalue ==1 :
        return 'STATUS'
    elif flagvalue == 2:
        return 'COMMAND'    
    elif flagvalue == 3:
        return 'USERINFO'
    else:
        return 'UNDEF'

def getMotorControllerRequestFlag(requesttype):    
    if requesttype =='STATUS' :
        return 1
    elif requesttype == 'COMMAND':
        return 2    
    elif requesttype == 'USERINFO':
        return 3
    elif requesttype == 'USERINFOUNIOCDE':
        return 4    
    else:
        return 0    
def GetMotorInfoDataType(dataflag):
        if dataflag == 1:
            return 'IRRSCHEINFO'
        elif dataflag ==2:
            return 'POWERINFO'
        elif dataflag == 3:
            return 'SENSORINFO'
        elif dataflag == 4:
            return 'STOP'
        elif dataflag == 5:
            return 'START'
        elif dataflag == 6:
            return 'DSTATUS'        
        elif dataflag == 7:
            return 'REGULARUPDATE'
        elif dataflag == 8:
            return 'ERRORUPDATE'
        elif dataflag == 9:
            return 'EPPROMUPDATE'
        elif dataflag == 10:
            return 'CALIBUPDATE'    
        elif dataflag == 11:
            return 'FNOUPDATE'
        elif dataflag == 12:
            return 'DNOUPDATE'                                                     
        else:
            return 'UNDEF'    
def GetMotorInfoDataTypeFlag(datatype):
        if datatype == 'IRRSCHEINFO':
            return 1
        elif datatype =='POWERINFO':
            return 2
        elif datatype == 'SENSORINFO':
            return 3
        elif datatype == 'STOP':
            return 4
        elif datatype =='START':
            return 5
        elif datatype ==' DSTATUS':
            return 6
        elif datatype == 'REGULARUPDATE':
            return 7
        elif datatype == 'ERRORUPDATE':
            return 8
        elif datatype == 'EPPROMUPDATE':
            return 9
        elif datatype == 'CALIBUPDATE':
            return 10
        elif datatype == 'FNOUPDATE':
            return 11
        elif datatype == 'DNOUPDATE':
            return 12                                                     
        else:
            return 0