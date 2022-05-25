import azure.functions as func
import json
import os
import psycopg2
import psycopg2.extras
import requests
from typing import List


def _get_predictions(reviews: List[str]) -> List[str]:
    response = requests.post(os.getenv('MODEL_URL'), json=reviews)
    response.raise_for_status()
    return json.loads(response.json())


def _db_save(reviews: List[str], predictions: List[str]) -> None:
    conn = psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASS'),
        dbname='postgres'
    )
    psycopg2.extras.execute_values(
        conn.cursor(),
        'INSERT INTO reviews (review, prediction) VALUES %s',
        zip(reviews, predictions)
    )
    conn.commit()


def main(req: func.HttpRequest) -> func.HttpResponse:
    reviews: List[str] = req.get_json()
    predictions: List[str] = _get_predictions(reviews)
    _db_save(reviews, predictions)
    return func.HttpResponse(json.dumps(predictions))
