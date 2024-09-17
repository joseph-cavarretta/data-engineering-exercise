import os
import sys
import logging
import time
import requests
import datetime
import boto3
from io import StringIO
import pandas as pd
from errors import MaxRetriesExceededError

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout)

DATE = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d')
EXTRACT_URL = 'https://openlibrary.org/subjects/fantasy.json?details=false'
S3_BUCKET = 'test-bucket'
S3_KEY = f'authors/full_processed_{DATE}.json'
ACCESS_KEY = os.getenv('aws_access_key', None)
SECRET_KEY = os.getenv('aws_secret_key', None)

# for demo purposes
OUT_PATH = f'./src/data/processed/full_processed_{DATE}.json'

COLUMNS = [
    'key',
    'title',
    'first_publish_year',
    'authors'
]

def main() -> None:
    data = extract_data(EXTRACT_URL)
    df = transform(data)
    cleanse_names(df)
    cleanse_book_key(df)
    add_surrogate_keys(df)
    upload_processed_to_local(df, OUT_PATH)
    #upload_processed_to_s3(df, S3_BUCKET, S3_KEY)


def extract_data(url: str, max_retries=2) -> dict:
    """ Sends an API request to specified url and returns json as dict"""
    for n in range(max_retries+1):
        try:
            logger.info(f'Sending request to open library api: attempt #{n+1}.')
            response = requests.get(url)
            # captures all success states, extend logic for specific cases
            response.raise_for_status()
            break
    
        except requests.exceptions.HTTPError as e:
            logger.error(e)
            if n != max_retries:
               backoff_duration = n+1**n
               logger.info(f'Retrying in {backoff_duration} seconds')
               time.sleep(backoff_duration)
            else:
                raise MaxRetriesExceededError('Max retries exceeded. Job failed.')
    
    logger.info('API call was successful.')
    return response.json()


def get_boto3_client() -> boto3.client:
    """ Instantiates boto3 s3 client """
    logger.info('Creating boto3 session.')
    return boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )


def transform(data: dict) -> pd.DataFrame:
    """ Formats raw json into dataframe """
    logger.info("Filtering and transforming json data.")
    data = data['works']
    filtered = [
        [
            record['key'],
            record['title'],
            record['first_publish_year'],
            # for books that have multiple authors
            [subrecord['name'] for subrecord in record['authors']]     
        ]
        for record in data
    ]
    
    df = pd.DataFrame(filtered, columns=COLUMNS)
    # explode authors column to give each author their own row
    df = df.explode('authors', ignore_index=True)
    df.rename(columns={'authors': 'author'}, inplace=True)
    logger.info(f"Dataframe created with {df.shape[0]} rows.")
    return df


def cleanse_names(df: pd.DataFrame) -> None:
    """ Modifies dataframe in place -> splits author name into first and last name fields"""
    logger.info("Cleansing author names.")
    df['author'] = df['author'].str.replace('.', '')
    df['author_firstname'] = df['author'].str.split(' ').str[:-1].str.join(' ')
    df['author_lastname'] = df['author'].str.split(' ').str[-1]
    df.drop_duplicates(inplace=True)
    logger.info(f"{df.shape[0]} rows remain after removing duplicates.")


def cleanse_book_key(df: pd.DataFrame) -> None:
    """ Modifies dataframe in place -> extracts book key from key column"""
    df['key'] = df['key'].str.split('/').str[-1]


def add_surrogate_keys(df: pd.DataFrame) -> None:
    """ Modifies dataframe in place -> adds surrogate keys for authors and books"""
    authors = {
        author: idx+1 for idx, author in enumerate(df.author.unique())
    }
    books = {
        key: idx+1 for idx, key in enumerate(df.key.unique())
    }
    df['author_id'] = df['author'].map(authors)
    df['book_id'] = df['key'].map(books)


def upload_processed_to_local(df: pd.DataFrame, path: str) -> None:
    """ For demo purposes write local file """
    logger.info('Writing processed data to local out path.')
    df.to_csv(path, index=False)


def upload_processed_to_s3(df: pd.DataFrame, bucket: str, key: str) -> None:
    """ Uploads dataframe to s3 bucket. Note: this is mocked for demo purposes and untested """
    s3 = get_boto3_client()
    logger.info(f'Attempting to write processed data to s3://{bucket}/{key}.')
    try:
        csv_buf = StringIO()
        df.to_csv(csv_buf, header=True, index=False)
        s3.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=key)
    except Exception as e:
        logger.error(f'Unable to write {key} to bucket: {bucket}')
        raise e


if __name__ == '__main__':
    main()