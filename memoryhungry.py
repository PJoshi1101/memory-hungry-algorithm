# -*- coding: utf-8 -*-
"""
Created on Fri Apr 12 19:08:41 2019

@author: pooja
@#python function get all the files of type jason 
@extract hashtag and language from json file
"""
import json
from collections import Counter
import my_spacesavingalgorithm as ss
from  multiprocessing import Process


#dataset path
day1 = ".\\all-tweets\\tweetsbydays\\day1" #tweets collected for day 1
day2 = ".\\all-tweets\\tweetsbydays\\day2" #tweets collected for day 2
day3 = ".\\all-tweets\\tweetsbydays\\day3" #tweets collected for day 3
   
def implement_MemoryHungry(filepath,imgsuffix):
       
   files = ss.get_filesfromdir(filepath)
   
   tagvalue = Counter()
   Hlang = Counter()
   Hwords = Counter()
   
   concatvalue = "%:-%"
                               
#  loop to read hashtag and language from each json file  
   for f in files:
       with open(f) as json_file:
           print("Processing file--",f)
           json_data = json.load(json_file)
           for line in json_data:
    #get hashtag data
                 hashtag = line['hashtags']
    #iterate through hashtage to get text data
                 for strtag in range(len(hashtag)):
                    strtagvalue = hashtag[strtag]['text']
                    Hlanguage = (strtagvalue +concatvalue + line['language'])
                    
                    if 'RT@:' in line['text'].strip():
                        data = line['text'].split(":")
                        data[1].strip()
                        finaldata = (data[1].split(" "))
                    else:
                        finaldata = (line['text'].strip()).split(" ")
                        
                    for item in finaldata:
                        if item.strip():
                            HText = (strtagvalue + concatvalue +item.strip())
                            if  HText in Hwords:
                                 Hwords[HText] = Hwords[HText] + 1
                            else:
                                Hwords[HText] = 1
                            
                    if strtagvalue in tagvalue:
                        tagvalue[strtagvalue] = tagvalue[strtagvalue] + 1
                    else:
                        tagvalue[strtagvalue] = 1
                        
                    if Hlanguage in Hlang :
                        Hlang[Hlanguage] = Hlang[Hlanguage] + 1
                    else:
                        Hlang[Hlanguage] = 1
    
##-------------------------------------------------------------------------------------------------------------   
#                        
   mhhashtag_dic = tagvalue.most_common(10)
   print(mhhashtag_dic)
   ss.plot(mhhashtag_dic,"Hashtag Value","Count","10-most frequent hashtags",imgsuffix+"mhhashtag.png")
   
   mhlang_dic = Hlang.most_common(10)
   print(mhlang_dic)
   ss.plot(mhlang_dic,"language","Count","10-most frequent languages in hashtag",imgsuffix+"mhlanguage.png")
   
   mhword_dic = Hwords.most_common(10)
   print(mhword_dic)
   ss.plot(mhword_dic,"words","Count","10-most frequent words in hashtag",imgsuffix+"mhwords.png")

#Function to execute memory hungry function
#In this function three process are created to run in parallel
#input - 3 day-wise dataset, number of counters, image suffix
if __name__ == '__main__':
    
    p1 = Process(target=implement_MemoryHungry, args=(day1,"day1"))    
    p2 = Process(target=implement_MemoryHungry, args=(day2,"day2"))
    p3 = Process(target=implement_MemoryHungry, args=(day3,"day3"))

#  starting a process
    p1.start()
    p2.start()
    p3.start()