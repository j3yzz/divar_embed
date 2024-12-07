import json

import openai
import logging
import requests
from dataclasses import dataclass
from typing import Optional

@dataclass
class PropertyQueryDTO:
    category_type: Optional[str] = None
    city: Optional[str] = None
    district: Optional[str] = None
    square_footage_min: Optional[int] = None
    square_footage_max: Optional[int] = None
    prepayment: Optional[float] = None
    price: Optional[float] = None


class OpenAIClient:
    def __init__(self, api_key: str):
        openai.api_key = api_key

    @staticmethod
    def generate_embedding(text: str):
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

    @staticmethod
    def generate_response(user_query, context):
        prompt = f"User Query: {user_query}\n\nHere are the top matches:\n{context}\n\nPlease suggest the best option in Persian."
        print(prompt)
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": prompt
                        }
                    ]
                },
            ],
            temperature=1,
            max_tokens=2048
        )

        return response.choices[0].message.content

    @staticmethod
    def generate_variables(user_query):
        prompt = (
            f"Extract all important keywords from following text and return in JSON format."
            f"JSON must include these keys: category_type, city, district, square_footage.min, square_footage.max, prepayment, price."
            f"Details: "
            f"1. category_type (include: apartment-sell, plot-old, house-villa-sell, apartment-rent, suite-apartment, villa, house-villa-rent, shop-rent, shop-sell, presell, office-rent, office-sell, workspace)"
            f"2. city (convert to persian)"
            f"3. district (convert to Finglish)"
            f"4. Range of Square footage (return format: min, max)"
            f"5. Prepayment and price (must be only number(integer))"
            f"User query: {user_query}"
        )

        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=1,
            response_format={
                "type": "json_object"
            },
            max_tokens=2048,
            top_p=1,
            frequency_penalty=0,
            presence_penalty=0
        )

        response_json = response.choices[0].message["content"]
        response_json = json.loads(response_json)

        return PropertyQueryDTO(
            category_type=response_json.get("category_type"),
            city=response_json.get("city"),
            district=response_json.get("district"),
            square_footage_min=response_json.get("square_footage", {}).get("min"),
            square_footage_max=response_json.get("square_footage", {}).get("max"),
            prepayment=response_json.get("prepayment"),
            price=response_json.get("price")
        )