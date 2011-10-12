#!/usr/bin/env python
import time, random,string,json,sys,urllib2


'''
{ "ver" : "1", "type" : "hits", "seq" : "0", 
"hits" : [{"ts" : "1276730926", "rt" : "5500", "cfq" : "2432", "sig" : "-86", "ant" : "1", "suid" : "QzQ6MEE6RDY6RTQ6MTI6NjA="},
          {"ts" : "1276730927", "rt" : "5500", "cfq" : "2432", "sig" : "-83", "ant" : "1", "suid" : "OUQ6QzM6REU6NTc6RjE6NDg=", "auid" : "M0E6OTQ6Q0Y6N0Q6Qjk6MjI="},
          {"ts" : "1276730927", "rt" : "5500", "cfq" : "2432", "sig" : "-83", "ant" : "1", "suid" : "OUQ6QzM6REU6NTc6RjE6NDg=", "auid" : "M0E6OTQ6Q0Y6N0Q6Qjk6MjI="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-84", "ant" : "1", "suid" : "MDg6OTQ6M0I6ODg6ODI6NTc="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-86", "ant" : "1", "suid" : "OEM6OEI6QzA6OUU6MzQ6MUE=", "auid" : "RUQ6NDE6N0M6MkI6RkU6RTI="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-85", "ant" : "1", "suid" : "MUU6OTU6Mjg6MzI6NTc6QjY=", "auid" : "NDk6RTM6Rjc6QjA6MEE6QjI="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-85", "ant" : "1", "suid" : "Q0Q6OEU6MDM6REI6QkI6QUQ=", "auid" : "MzA6QjU6QkQ6RDg6ODA6MTE="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-83", "ant" : "1", "suid" : "ODM6MTA6MDA6MTM6NDI6QTQ="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-82", "ant" : "1", "suid" : "NDY6RUQ6OUM6QzY6MDA6MDA=", "auid" : "MDA6MDA6MDA6MDA6MDA6MDA="},
          {"ts" : "1276730928", "rt" : "5500", "cfq" : "2432", "sig" : "-83", "ant" : "1", "suid" : "NkQ6Mjg6MUI6MEM6MDA6MDA="}]}
'''

def random_mac():
    mac = [ 0x00, 0x16, 0x3e,  
           random.randint(0x00, 0x7f),  
           random.randint(0x00, 0xff),  
           random.randint(0x00, 0xff) ]  
    return ':'.join(map(lambda x: "%02x" % x, mac))  

def random_mac_str():
    return "".join([random.choice(string.letters) for x in range(24)])

def generate_euclid_data(num_hits):
    result = {"ver" : "1", "type" : "hits", "seq" : "0", 'hits':[]}
    for i in range(num_hits):
        this_hit = {}
        this_hit['ts'] = long(time.time())
        this_hit['rt'] = "5500"
        this_hit['sig'] = "-8" + str(random.randint(0,9))
        this_hit['ant'] = "1"
        this_hit['cfg'] = "2432"
        this_hit['suid'] = random_mac()
        this_hit['auid'] = random_mac()
        result['hits'].append(this_hit)
    return json.dumps(result) 



if __name__ == '__main__':
    print "payload="+generate_euclid_data(100)        