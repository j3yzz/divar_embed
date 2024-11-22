import openai
import logging
import requests

class OpenAIClient:
    def __init__(self, api_key: str):
        openai.api_key = api_key

    def generate_embedding(self, text: str):
        try:
            response = openai.Embedding.create(
                input=text,
                model="text-embedding-ada-002",
                timeout=30
            )
            return response['data'][0]['embedding']
        except requests.exceptions.RequestException as e:
            logging.error(f"network error occurred while generating embedding: {e}")
            raise
        except openai.error.OpenAIError as e:
            logging.error(f"OpenAI API error occurred: {e}")
            raise