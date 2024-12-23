import json

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

    @staticmethod
    def build_query(PropertyQueryDTO):
        print("PropertyQueryDTO", PropertyQueryDTO)
        query = {
            "bool": {"must": [], "filter": []}
        }

        if PropertyQueryDTO.keywords:
            query["bool"]["must"].append({"query_string": {"default_field": "ad_description", "query": PropertyQueryDTO.keywords}})

        if PropertyQueryDTO.city:
            query["bool"]["must"].append({"term": {"original_copy.city.name.keyword": PropertyQueryDTO.city}})

        if PropertyQueryDTO.category_type:
            query["bool"]["must"].append({"term": {"categories.cat_3.keyword": PropertyQueryDTO.category_type}})

        if PropertyQueryDTO.square_footage_min or PropertyQueryDTO.square_footage_max:
            min_sqft, max_sqft = (PropertyQueryDTO.square_footage_min, PropertyQueryDTO.square_footage_max)
            if min_sqft is None:
                min_sqft = 0
            if max_sqft is None:
                max_sqft = 100000

            query["bool"]["filter"].append(
                {"range": {"original_copy.seo.post_seo_schema.floorSize.value": {"gte": min_sqft, "lte": max_sqft}}})

        if PropertyQueryDTO.prepayment:
            query["bool"]["filter"].append({"range": {"original_copy.webengage.credit": {"gte": PropertyQueryDTO.prepayment}}})

        if PropertyQueryDTO.price:
            query["bool"]["filter"].append({"range": {"original_copy.webengage.rent": {"gte": PropertyQueryDTO.prepayment}}})

        query["bool"]["must"].append({"range": {"document_inserted_at": {"gte": "now-5d/d"}}})

        return query

    def search_elasticsearch(self,
                             index_name,
                             PropertyQueryDTO):
        query = self.build_query(PropertyQueryDTO)
        print("query", query)

        response = self.es.search(
            index=index_name,
            body={
                "query": query,
                "_source": [
                    "ad_description",
                    "title",
                    "post_token",
                    "images",
                    "extracted_categories",
                    "categories.credit",
                ]
            },
            size=10)

        return response["hits"]["hits"]
