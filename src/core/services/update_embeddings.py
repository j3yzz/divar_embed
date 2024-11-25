from typing import List
from ..interfaces.elasticsearch_client import ElasticsearchClient
from ..interfaces.openai_client import OpenAIClient
from ..entities.document import Document
import logging


class UpdateEmbeddings:
    def __init__(self, es_client: ElasticsearchClient, openai_client: OpenAIClient):
        self.es_client = es_client
        self.openai_client = openai_client

    def generate_embedding(self, text: str):
        return self.openai_client.generate_embedding(text)

    def fetch_documents_without_embeddings(self, index_name: str, batch_size: int = 100) -> List[Document]:
        documents = self.es_client.fetch_documents_without_embeddings(index_name, batch_size)
        return [
            Document(
                doc["_id"],
                doc["_source"].get("title", ""),
                doc["_source"].get("ad_description", ""),
                doc["_source"].get("categories", {}),
                doc["_source"].get("location", ""),
                doc["_source"].get("price", "")
            )
            for doc in documents
        ]

    def prepare_combined_text(self, title: str, ad_description: str, categories: dict, location: str, price: str) -> str:
        category_text = ", ".join([
            categories.get("cat_1", ""),
            categories.get("cat_2", ""),
            categories.get("cat_3", "")
        ]).strip(", ")

        combined_text = f"{title}با قیمت {price} در {location} با دسته بندی {category_text}{ad_description}. توضیحات: "
        return combined_text.strip(" - ")

    def update_documents_with_embeddings(self, index_name: str, documents: List[Document]):
        actions = []
        for doc in documents:
            try:
                combined_text = self.prepare_combined_text(
                    title=doc.title,
                    ad_description=doc.ad_description,
                    categories=doc.categories,
                    location=doc.location,
                    price=doc.price
                )

                if not combined_text:
                    logging.warning(f"Skipping document {doc.doc_id} due to empty combined text")
                    continue

                embedding = self.generate_embedding(combined_text)
                doc.embedding_vector = embedding
                action = {
                    "_op_type": "update",
                    "_index": index_name,
                    "_id": doc.doc_id,
                    "doc": {"embedding_vector": doc.embedding_vector},
                }
                actions.append(action)
                logging.info(f"Processed document {doc.doc_id}")
            except Exception as e:
                logging.error(f"Error generating embedding for document {doc.doc_id}: {e}")
        if actions:
            self.es_client.bulk_update(index_name, actions)
