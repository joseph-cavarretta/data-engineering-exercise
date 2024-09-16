import os
import sys
import logging
import datetime
import pandas as pd
import boto3

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout)

DATE = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d')
S3_BUCKET = 'test-bucket'
PROCESSED_S3_KEY = f'processed/full_processed_{DATE}.csv'
ACCESS_KEY = os.getenv('aws_access_key', None)
SECRET_KEY = os.getenv('aws_secret_key', None)

# for demo purposes
IN_PATH = f'./src/data/processed/full_processed_{DATE}.json'
OUT_AUTHORS = f'./src/data/authors/authors_{DATE}.csv'
OUT_BOOKS = f'./src/data/books/books_{DATE}.csv'

def main() -> None:
    data = load_data_from_local(IN_PATH)
    #data = load_data_s3(S3_BUCKET, RAW_S3_KEY)
    generate_authors_table(data)
    generate_books_table(data)


def get_boto3_client() -> boto3.client:
    """ Instantiates boto3 s3 client """
    return boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )


def load_data_from_local(path) -> pd.DataFrame:
    """ For demo purposes load local file """
    logger.info('Reading data from local in path.')
    return pd.read_csv(path)


def load_data_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """ Load processed data from s3. Note: this is mocked for demo purposes and untested """
    s3 = get_boto3_client()
    s3_obj = s3.get_object(Bucket=bucket, Key=key) 
    text = s3_obj["Body"].read().decode()
    return pd.read_csv(text)


def insert_records(data: list, table: str, columns: str) -> None:
    """ Mocked for demo. Inserts a dataframe to database as a table """
    # creds = get_db_creds()
    # with conn as db_connect(creds):
    #   cursor = conn.cursor()
    #   query = f'INSERT INTO {table} ({columns}) VALUES %s'
    #   execute_values(cursor, query, data)
    pass

def generate_authors_table(df: pd.DataFrame) -> pd.DataFrame:
    """ Generates a full refresh authors dimensional table from dataframe """
    authors = df.copy(deep=True)
    authors['created_datetime'] = pd.Timestamp('now')
    authors = authors[['author_id', 'author', 'author_firstname', 'author_lastname', 'created_datetime']]
    records = authors.to_records(index=False).tolist()
    col_str = ','.join(authors.columns)
    insert_records(records, 'authors',col_str)
    authors.to_csv(OUT_AUTHORS, index=False)


def generate_books_table(df: pd.DataFrame) -> pd.DataFrame:
    """ Generates a full refresh books dimensional table from dataframe """
    books = df.copy(deep=True)
    books['created_datetime'] = pd.Timestamp('now')
    books = books[['book_id', 'author_id', 'key', 'title', 'first_publish_year', 'created_datetime']]
    records = books.to_records(index=False).tolist()
    col_str = ','.join(books.columns)
    insert_records(records, 'books', col_str)
    books.to_csv(OUT_BOOKS, index=False)



if __name__ == '__main__':
    main()