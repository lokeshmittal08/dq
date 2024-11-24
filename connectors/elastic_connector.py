import json
import pandas as pd
from elasticsearch import Elasticsearch

def get_elasticsearch_client(host):
    return Elasticsearch(hosts=[host])

def fetch_data_from_elastic(client, index, query):
    parsed_query = json.loads(query)
    response = client.search(index=index, body={"query": parsed_query}, size=10000)
    hits = response["hits"]["hits"]
    return pd.DataFrame([hit["_source"] for hit in hits])
