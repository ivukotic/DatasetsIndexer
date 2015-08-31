from elasticsearch import Elasticsearch
from elasticsearch import helpers
from datetime import datetime

print "reading the data"
with open('dsnames.txt', 'r') as f:
    read_data = f.readlines()

dss=read_data[2:]

es = Elasticsearch("uct2-es-head:9200")

#es.indices.delete(index='local_group_disk_datasets', ignore=[400, 404])

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index='local_group_disk_datasets_'+str(datetime.now().date()), ignore=400)

def decodeDT(ds):
    wo=ds.split(".")
    for w in wo:
        if w.startswith("DESD_") or w.startswith("DAOD_"): return w; 
    return "unknown"

actions = []

for DS in dss:
    scope=""
    user="unknown"
    group="unknown"
    datatype="unknown"
    
    if DS.count(':')>0:
        spl=DS.split(':')
        scope=spl[0]
        DS=spl[1]
    else:
        w=DS.split('.')
        if w[0].startswith('user'):
            user=w[1]
            scope=w[0]+'.'+w[1]
        elif w[0].startswith('group'):
            group=w[1]
            scope=w[0]+'.'+w[1]
        else:
            scope=w[0]
    
    if DS.count(".AOD.")>0: datatype="AOD"
    elif DS.count(".ESD.")>0: datatype="ESD"
    elif DS.count(".RAW")>0: datatype="RAW"
    else datatype=decodeDT(DS)
    
    action = {
        "_index": "local_group_disk_datasets",
        "_type": "DS",
        "_id": j,
        "_source": {
            "scope":scope,
            "fn":DS,
            "user":user,
            "group":group,
            "type":datatype,
            "timestamp": datetime.now()
            }
        }
    actions.append(action)

helpers.bulk(es, actions)