#!/usr/bin/env python
# -*- coding: utf-8 -*-
import apistatic
from mtranslate import translate
class Language(object):    
    def gethex(self,inputmessage):        
        if apistatic.SMSTYPE =='USERINFOUNIOCDE':
            localmessage = ''
            if apistatic.LANGUAGE =='TELUGU':            
                localmessage = translate(inputmessage,'te')                
            if apistatic.LANGUAGE =='BENGAL':            
                localmessage = translate(inputmessage,'bn')     
            if apistatic.LANGUAGE =='HINDI':            
                localmessage = translate(inputmessage,'hi')                                                               
            localmessage=localmessage.encode('utf-16-le').encode('hex').upper()
            resulthex = ""
            for x in xrange(0, len(localmessage), 4):
                resulthex= resulthex  + localmessage[x+2:x+4] + localmessage[x:x+2]                            
            return resulthex
        else:
            return inputmessage   
    def get(self,inputmessage):        
        if apistatic.SMSTYPE =='USERINFOUNIOCDE':
            if apistatic.LANGUAGE =='TELUGU':            
                localmessage = translate(inputmessage,'te')           #translating language using mtranslator             
                return localmessage
            if apistatic.LANGUAGE =='BENGAL':            
                localmessage = translate(inputmessage,'bn')                        
                return localmessage
            if apistatic.LANGUAGE =='HINDI':            
                localmessage = translate(inputmessage,'hi')                        
                return localmessage            
        else:
            return inputmessage                 
     
