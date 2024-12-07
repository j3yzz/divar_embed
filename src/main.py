import os
from dotenv import load_dotenv
from core.interfaces.elasticsearch_client import ElasticsearchClient
from core.interfaces.openai_client import OpenAIClient
from core.services.update_embeddings import UpdateEmbeddings

load_dotenv()

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
ELASTICSEARCH_HOST = os.getenv("ELASTICSEARCH_HOST")
INDEX_NAME = os.getenv("ELASTICSEARCH_INDEX_NAME")



def prepare_context(results):
    context = []
    for doc in results:
        source = doc["_source"]
        context.append(
            f"Title: {source['title']}\n"
            f"Description: {source['ad_description']}\n"
            f"Price: {source['categories']['rent']} Toman\n"
            f"Location: {source['location']}\n"
            f"URL: {source['url']}\n"
            f"Post Token: {source['post_token']}"
        )
    return "\n\n".join(context)

def main():
    es_client = ElasticsearchClient([ELASTICSEARCH_HOST])
    openai_client = OpenAIClient(OPENAI_API_KEY)


    user_query = "من یک آپارتمان برای اجاره و رهن در ناحیه‌ی مرزداران تهران میخواهم که حدودا بین ۱۰۰ تا ۱۵۰ متر باشد و ده میلیارد ریال به عنوان پیش پرداخت (رهن) و ماهانه ۱۰۰ میلیون ریال اجاره بدهم."

    varResponse = openai_client.generate_variables(user_query=user_query)

    print(varResponse)

    # name = "آپارتمان"
    # location = "تهران"
    # rental_type = "house-villa-rent"
    # square_footage = (60, 200)
    # price = 1000000000
    #
    # results = es_client.search_elasticsearch(name, location, rental_type, square_footage, price, INDEX_NAME)
    #
    # if not results:
    #     print("No results found for your query.")
    #     exit(1)
    #
    # context = prepare_context(results)
    #
    # response = openai_client.generate_response(user_query, context)
    #
    # print(response)


if __name__ == "__main__":
    main()