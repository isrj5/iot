'''Author : venkatanaidu.mudadla@gmail.com , at kisan raja @2017'''
def skipMotordataflags(datatype,dataflag):
    if datatype == 'REGULARUPDATE':
        if dataflag == 0:
            return dataflag + 3
        if dataflag == 6 :
            return dataflag + 3
        if dataflag == 12:
            return dataflag + 3
    if datatype == 'ERRORUPDATE':
        if datatype == 12:
            return dataflag +3 
    if datatype == 'EPPROMUPDATE':
        if datatype == 0:
            return dataflag + 21    
        if datatype == 22:
            return dataflag + 1  
    if datatype == 'CALIBUPDATE':
        if dataflag == 0 :      
            return dataflag + 3    
        if dataflag == 6 :
            return dataflag + 3                 
        if dataflag == 15 :
            return dataflag + 7  
        if dataflag == 23 :
                return dataflag -4
    if datatype == 'FNOUPDATE':
        if dataflag == 0:
            return dataflag + 26
        if dataflag == 28:
            return dataflag -3    
    if datatype == 'DNOUPDATE':
        if dataflag == 0:
            return dataflag + 26                           
    return dataflag        
def bytetonum(byte):
    return int(byte,16)
def getchar(byte):        
    return chr(int(byte,16))
def getnumber(msb,lsb):
    return  int(msb + lsb,16)
def getmobileNumber(mobilenum):    
    mobile =''
    for byte in mobilenum:
        if len(mobile)>9:
            break;
        mobile = mobile + chr(int(byte,16))
    return mobile    
def calibrate(sensorName,value,addOns):        
    result = -1
    if sensorName == 'water-level':       
        level = (float(addOns['consk'])*(float(value))  +  float(addOns['consc']))/10
        result=64-(level+30)
    elif sensorName =='soil-moisture':        
        result = (float(addOns['conssoilmax']) / float(addOns['conswet']) ) * (float(value)/100)
    else:
        result  = value
    return result