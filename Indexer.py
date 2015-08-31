from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime

print "reading the data"
with open('dsnames.txt', 'r') as f:
    read_data = f.readlines()

dss=read_data[2:]

es = Elasticsearch("uct2-es-head:9200")

IndName='local_group_disk_datasets_'+str(datetime.now().date())
es.indices.delete(index=IndName, ignore=[400, 404])

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index=IndName, ignore=400)

def decodeDT(ds):
    wo=ds.split(".")
    for w in wo:
        if w.startswith("DESD_") or w.startswith("DAOD_"): return w; 
    return "unknown"

actions = []

for DS in dss:
    DS=DS.strip()
    scope=""
    creator="unknown"
    datatype="unknown"
    
    spl=DS.split(':')
    scope=spl[0]
    DS=spl[1]
    
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
            "timestamp": datetime.now()
            }
        }
    actions.append(action)

helpers.bulk(es, actions)