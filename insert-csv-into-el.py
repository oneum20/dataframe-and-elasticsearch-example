import numpy as np
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch import helpers

# Set variables
csv_file = input("CSV Path : ")

ip = input("ElasticSearch Server IP : ")
port = input("ElasticSearch Server Port : ")

idx = input("ElasticSearch Index : ")
type = input("ElasticSearch Type : ")


# get csv data
data = pd.read_csv(csv_file).replace(np.NaN, '', regex=True)


# config el info
es = Elasticsearch(host=ip, port=port)


# check index & create index
if not es.indices.exists(index=idx):
    print("Not exist index... Create index : ", idx)
    es.indices.create(index=idx,body={})
 

 # insert df into el
documents = data.to_dict(orient='records')
helpers.bulk(es, documents, index=idx, doc_type=type, raise_on_error=True)
