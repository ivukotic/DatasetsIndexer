from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime

print "reading the data"
with open('dsnames.txt', 'r') as f:
    read_data = f.readlines()

dss=read_data[2:]

import os, Queue
from threading import Thread
import rucio
import rucio.client
import rucio.common.config as conf


if os.environ.get("RUCIO_ACCOUNT") != None:
    rucio_account=os.environ.get("RUCIO_ACCOUNT")
else:
    print "no RUCIO_ACCOUNT environment found. Please set it before using this program."
    sys.exit(1) 

class DataSet:
    def __init__(self,line):
        self.creator="unknown"
        self.datatype="unknown"
        spl=line.split(':')
        self.scope=spl[0]
        self.name=spl[1]    
        self.size=0
        self.files=0
        
        w=self.scope.split('.')
        if w[0].startswith('user') or w[0].startswith('group'):
            self.creator=w[1]
            
        if self.name.count(".AOD.")>0: self.datatype="AOD"
        elif self.name.count(".ESD.")>0: self.datatype="ESD"
        elif self.name.count(".RAW")>0: self.datatype="RAW"
        elif self.name.count(".RDO")>0: self.datatype="RDO"
        wo=self.name.split(".")
        for w in wo:
            if w.startswith("DESD_") or w.startswith("DAOD_"): 
                self.datatype=w;
                break 
                
    def getFilesSize(self):
        cont=rucio.client.didclient.DIDClient().get_did(self.scope,self.name)
        self.files = cont['length']
        self.size  = cont['bytes']      
        
    def prnt(self):
        print self.scope,self.name, self.size, self.files, self.creator, self.datatype  

rrc=rucio.client.replicaclient.ReplicaClient()


    
def worker():
    while True:
        f=q.get()
        f.getFilesSize()
        f.prnt()
        print "still in queue: ", q.qsize()
        q.task_done()


es = Elasticsearch("uct2-es-head:9200")

IndName='temp_local_group_disk_datasets_'+str(datetime.now().date())
es.indices.delete(index=IndName, ignore=[400, 404])

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index=IndName, ignore=400)


# created threads
q=Queue.Queue()
for i in range(10):
     t = Thread(target=worker)
     t.daemon = True
     t.start()
     
DataSets=[]
for DS in dss:
    DS=DS.strip()
    nds=DataSet(DS)
    DataSets.append(DataSet(DS))
    q.put(nds)

q.join()     

print "indexing ..."
actions = []
for ds in DataSets:
    action = {
        "_index": IndName,
        "_type": "DS",
        "_source": {
            "scope":ds.scope,
            "fn":ds.name,
            "creator":ds.creator,
            "type":ds.datatype,
            "files":ds.files,
            "size":ds.size,
            "timestamp": datetime.now()
            }
        }
    actions.append(action)

print actions
helpers.bulk(es, actions)