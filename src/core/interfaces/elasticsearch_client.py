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
    def build_query(location=None, rental_type=None, square_footage=None, price=None, prepayment=None, keywords=None):
        query = {
            "bool": {"must": [], "filter": []}
        }

        if keywords:
            query["bool"]["must"].append({"query_string": {"default_field": "ad_description", "query": keywords}})

        if location:
            query["bool"]["must"].append({"term": {"original_copy.city.name.keyword": location}})

        if rental_type:
            query["bool"]["must"].append({"term": {"categories.cat_3.keyword": rental_type}})

        # if square_footage:
        #     min_sqft, max_sqft = square_footage
        #     query["bool"]["filter"].append(
        #         {"range": {"seo.post_seo_schema.floorSize.value": {"gte": min_sqft, "lte": max_sqft}}})

        if price:
            max_price = price
            query["bool"]["filter"].append({"range": {"categories.rent": {"lte": max_price}}})
            query["bool"]["filter"].append({"range": {"categories.credit": {"lte": max_price}}})
            query["bool"]["filter"].append({"range": {"categories.price": {"lte": max_price}}})

        query["bool"]["must"].append({"range": {"document_inserted_at": {"gte": "now-5d/d"}}})

        return query

    def search_elasticsearch(self,
                             location=None,
                             rental_type=None,
                             square_footage=None,
                             price=None,
                             prepayment=None,
                             keywords=None,
                             index_name=None):
        query = self.build_query(location, rental_type, square_footage, price, prepayment, keywords)
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
