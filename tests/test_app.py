from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.app_functions import filtering, rename_columns
import pytest
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture()
def session():
    return SparkSession.builder.appName('test').getOrCreate()

@pytest.fixture()
def input_schema():
    return StructType([
        StructField('id', IntegerType(), True),
        StructField('email', StringType(), True),
        StructField('country', StringType(), True),
        StructField('btc_a', StringType(), True),
        StructField('cc_t', StringType(), True)
    ])

@pytest.fixture()
def output_schema():
    return StructType([
        StructField('client_identifier', IntegerType(), True),
        StructField('email', StringType(), True),
        StructField('country', StringType(), True),
        StructField('bitcoin_address', StringType(), True),
        StructField('credit_card_type', StringType(), True)
    ])

@pytest.fixture()
def test_data():
    return [
            (1,"dummy@gmail.com", "France", "dummy-transaction", "mastercard"),
            (18, "some-box@mail.ru", "United Kingdom", "some-case", "visa"),
            (20, "simple-mail@outlook.com", "France", "imaginary-purchase", "american-express"),
            (36, "hoax@gmail.com", "Netherlands", "another-dummy-transaction", "visa"),
            (62, "figment@mail.ru", "Netherlands", "mock-deal", "visa")
    ]


@pytest.fixture()
def test_df(session, test_data, input_schema):
    return session.createDataFrame(data=test_data, schema=input_schema)

@pytest.fixture()
def output_df(session, test_data, output_schema):
    return session.createDataFrame(data=test_data, schema=output_schema)

def test_filtering(session, test_df, input_schema):
    expected_data = [
            (18, "some-box@mail.ru", "United Kingdom", "some-case", "visa"),
            (36, "hoax@gmail.com", "Netherlands", "another-dummy-transaction", "visa"),
            (62, "figment@mail.ru", "Netherlands", "mock-deal", "visa")
    ]
    expected_df = session.createDataFrame(data=expected_data,
                                           schema=input_schema)
    filtered_df = filtering(test_df, 'United Kingdom,Netherlands')
    assert_df_equality(expected_df, filtered_df)

def test_rename_columns(test_df, output_df):
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }
    renamed_df = rename_columns(test_df, column_mapping)
    assert_df_equality(renamed_df, output_df)
