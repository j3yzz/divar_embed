import os
from dotenv import load_dotenv
from fastapi import FastAPI

from core.interfaces.elasticsearch_client import ElasticsearchClient
from core.interfaces.openai_client import OpenAIClient
from pydantic import BaseModel
from fastapi.middleware.httpsredirect import HTTPSRedirectMiddleware

from fastapi.middleware.cors import CORSMiddleware

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


app = FastAPI()

origins = ["*"]

app.add_middleware(HTTPSRedirectMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Payload(BaseModel):
    prompt: str


@app.post("/process")
async def process_payload(payload: Payload):
    es_client = ElasticsearchClient([ELASTICSEARCH_HOST])
    openai_client = OpenAIClient(OPENAI_API_KEY)

    user_query = payload.prompt

    userQueryExtractedVariables = openai_client.generate_variables(user_query=user_query)

    response = es_client.search_elasticsearch(
        location=userQueryExtractedVariables.city,
        rental_type=userQueryExtractedVariables.category_type,
        square_footage=(userQueryExtractedVariables.square_footage_min, userQueryExtractedVariables.square_footage_max),
        price=userQueryExtractedVariables.price,
        prepayment=userQueryExtractedVariables.prepayment,
        keywords=userQueryExtractedVariables.keywords,
        index_name=INDEX_NAME
    )
    return {"status": "ok", "data": [
        {
            "ad_description": doc["_source"]["ad_description"],
            "title": doc["_source"]["title"],
            "ad_link": "https://divar.ir/v/" + doc["_source"]["post_token"],
            "images": doc["_source"]["images"][0].get("url", {}),
            "extracted_categories": doc["_source"]["extracted_categories"],
            "credit": doc["_source"]["categories"].get("credit"),
        }
        for doc in response
    ]}
