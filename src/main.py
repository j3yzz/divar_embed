import os
from dotenv import load_dotenv
from core.interfaces.elasticsearch_client import ElasticsearchClient
from core.interfaces.openai_client import OpenAIClient
from core.services.update_embeddings import UpdateEmbeddings

load_dotenv()

def main():
    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
    ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
    INDEX_NAME = os.getenv("ELASTICSEARCH_INDEX_NAME")

    if not OPENAI_API_KEY or not ELASTICSEARCH_HOST or not INDEX_NAME:
        raise ValueError("Missing one or more required environment variables")

    es_client = ElasticsearchClient([ELASTICSEARCH_HOST])
    openai_client = OpenAIClient(OPENAI_API_KEY)

    update_embeddings_usecase = UpdateEmbeddings(es_client, openai_client)

    print("Fetching documents without embeddings...")
    documents = update_embeddings_usecase.fetch_documents_without_embeddings(INDEX_NAME)

    batch = []
    batch_size = 50

    for doc in documents:
        batch.append(doc)
        if len(batch) >= batch_size:
            print(f"Processing batch of {len(batch)} documents...")
            update_embeddings_usecase.update_documents_with_embeddings(INDEX_NAME, batch)
            batch = []

    if batch:
        print(f"Processing final batch of {len(batch)} documents...")
        update_embeddings_usecase.update_documents_with_embeddings(INDEX_NAME, batch)

    print("Completed embedding updates.")

if __name__ == "__main__":
    main()
