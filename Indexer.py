from elasticsearch import Elasticsearch
es = Elasticsearch("uct2-es-head:9200")

es.indices.delete(index='LocalGroupDiskDatasets', ignore=[400, 404])

# ignore 400 cause by IndexAlreadyExistsException when creating an index
es.indices.create(index='LocalGroupDiskDatasets', ignore=400)

# add a doc
es.indices.index(index='LocalGroupDiskDatasets',doc_type="ds",body="")

actions = []
while (j <= 10):
    action = {
        "_index": "LocalGroupDiskDatasets",
        "_type": "DS",
        "_id": j,
        "_source": {
            "scope":"user.yang43",
            "fn":"user.yang43.data11_7TeV.DAOD_ONIAMUMU.Period_M2M4M5M6M8M10.pro10_v01.Onia.17.0.4.1.v1.4.131226095813" + str(j),
            "timestamp": datetime.now()
            }
        }
    actions.append(action)
    j += 1

helpers.bulk(es, actions)