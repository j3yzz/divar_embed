from elasticsearch import Elasticsearch, helpers

class ElasticsearchClient:
    def __init__(self, hosts, timeout=30):
        self.es = Elasticsearch(hosts, headers={"Content-Type": "application/json"}, timeout=timeout)

    def fetch_documents_without_embeddings(self, index_name: str, batch_size: int = 100):
        query = {
            "query": {
                "bool": {
                    "must_not": {
                        "exists": {
                            "field": "embedding_vector"
                        }
                    },
                    "must": [
                        {
                            "range": {
                                "document_inserted_at": {
                                    "gte": "now-10m/m"
                                }
                            }
                        }
                    ]
                }
            }
        }

        return helpers.scan(self.es, query=query, index=index_name, size=batch_size)

    def bulk_update(self, index_name: str, actions):
        helpers.bulk(self.es, actions)

