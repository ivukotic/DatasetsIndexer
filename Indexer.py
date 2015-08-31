from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime

print "reading the data"
with open('dsnames.txt', 'r') as f:
    read_data = f.readlines()

dss=read_data[2:]


import rucio
import rucio.client
import rucio.common.config as conf


if os.environ.get("RUCIO_ACCOUNT") != None:
    rucio_account=os.environ.get("RUCIO_ACCOUNT")
else:
    print "no RUCIO_ACCOUNT environment found. Please set it before using this program."
    sys.exit(1) 

rrc=rucio.client.replicaclient.ReplicaClient()


def decodeDT(ds):
    wo=ds.split(".")
    for w in wo:
        if w.startswith("DESD_") or w.startswith("DAOD_"): return w; 
    return "unknown"

def getFilesSize(scope, DS):
    coll=[0,0]
    cont=rucio.client.didclient.DIDClient().list_content(scope,DS)
    for f in cont:
        if f['type']=='DATASET':
            collected=getFiles(f['scope'],f['name']) 
            coll[0]+=collected[0]
            coll[1]+=collected[1]
        else:
            coll[0]+=1
            coll[1]+=f['bytes']
    return coll
    

es = Elasticsearch("uct2-es-head:9200")

IndName='Tlocal_group_disk_datasets_'+str(datetime.now().date())
es.indices.delete(index=IndName, ignore=[400, 404])

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index=IndName, ignore=400)

actions = []

for DS in dss:
    DS=DS.strip()
    scope=""
    creator="unknown"
    datatype="unknown"
    
    spl=DS.split(':')
    scope=spl[0]
    DS=spl[1]
    
    filesSizes=getFilesSize(scope,DS)
    
    w=scope.split('.')
    if w[0].startswith('user') or w[0].startswith('group'):
        creator=w[1]
    
    if DS.count(".AOD.")>0: datatype="AOD"
    elif DS.count(".ESD.")>0: datatype="ESD"
    elif DS.count(".RAW")>0: datatype="RAW"
    elif DS.count(".RDO")>0: datatype="RDO"
    else: datatype=decodeDT(DS)
    print scope, creator, datatype, DS
    action = {
        "_index": IndName,
        "_type": "DS",
        # "_id": j,
        "_source": {
            "scope":scope,
            "fn":DS,
            "creator":creator,
            "type":datatype,
            "files":filesSizes[0],
            "size":filesSizes[1],
            "timestamp": datetime.now()
            }
        }
    actions.append(action)

helpers.bulk(es, actions)