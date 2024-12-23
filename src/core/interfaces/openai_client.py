import json
from http.client import responses

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
    keywords: Optional[str] = None
    number_of_rooms: Optional[int] = None
    has_elevator: Optional[bool] = None
    has_parking: Optional[bool] = None
    year_of_manufacture: Optional[int] = None
    floor_number: Optional[int] = None


class OpenAIClient:
    def __init__(self, api_key: str):
        openai.api_key = api_key

    @staticmethod
    def generate_variables(user_query):
        prompt = (
            f"Extract all important keywords from following text and return in JSON format."
            f"JSON must include these keys:"
            f"category_type, city, square_footage.min, square_footage.max, prepayment, price, keywords, number_of_rooms, has_elevator, has_parking, year_of_manufacture, floor_number."
            f"Details:"
            f"1. category_type (include: apartment-sell, plot-old, house-villa-sell, apartment-rent, suite-apartment, villa, house-villa-rent, shop-rent, shop-sell, presell, office-rent, office-sell, workspace)"
            f"2. city (convert to persian)"
            f"3. Range of Square footage (return format: min, max)"
            f"4. Prepayment and price (must be only number(integer))"
            f"5. Extract Keywords with OR for Elasticsearch Query"
            f"6. number_of_rooms: Number of rooms"
            f"7. Has Elevator must be 0 or 1"
            f"8. has parking must be 0 or 1 ( if not in prompt, return null )"
            f"9. Year of manufacture must be integer"
            f"10. Floor_number must be Integer"
            f"User query: {user_query}"
        )

        response = openai.chat.completions.create(
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

        response_json = response.choices[0].message.content
        response_json = json.loads(response_json)
        print("response_json", response_json)

        print("\n\n\n")
        print("-----------------")
        print("\n\n\n")

        return PropertyQueryDTO(
            category_type=response_json.get("category_type"),
            city=response_json.get("city"),
            district=response_json.get("district"),
            square_footage_min=response_json.get("square_footage", {}).get("min"),
            square_footage_max=response_json.get("square_footage", {}).get("max"),
            prepayment=response_json.get("prepayment"),
            price=response_json.get("price"),
            keywords=response_json.get("keywords"),
            number_of_rooms=response_json.get("number_of_rooms"),
            has_elevator=response_json.get("has_elevator"),
            has_parking=response_json.get("has_parking"),
            year_of_manufacture=response_json.get("year_of_manufacture"),
            floor_number=response_json.get("floor_number"),
        )
