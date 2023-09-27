from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from functions import filtering, rename_columns
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
        StructField('counrty', StringType(), True),
        StructField('btc_a', StringType(), True),
        StructField('cc_t', StringType(), True)
    ])

@pytest.fixture()
def output_schema():
    return StructType([
        StructField('client_identifier', IntegerType(), True),
        StructField('email', StringType(), True),
        StructField('counrty', StringType(), True),
        StructField('bitcoin_address', StringType(), True),
        StructField('credit_card_type', StringType(), True)
    ])

@pytest.fixture()
def test_data():
    return [
            (1,"feusden0@ameblo.jp", "France", "1wjtPamAZeGhRnZfhBAHHHjNvnHefd2V2", "visa-electron"),
            (18, "rdrinanh@odnoklassniki.ru", "United Kingdom", "1ErM8yuF3ytzzxLy1uPvQuRveLBygxN15x", "china-unionpay"),
            (20, "cmartinettoj@mapy.cz", "France", "14iPptCE59bQXGoczEsSkk4ejTyaKq9mxt", "jcb"),
            (36, "dbuckthorpz@tmall.com", "Netherlands", "15X53Z9B9jUNrvFpbr7D554uSc5RL7Pnkg", "diners-club-international"),
            (62, "bbarham1p@wisc.edu", "Netherlands", "16qpYVt6YAAx4JYjzbA8SwTUHoTyB4twRF", "jcb")
    ]


@pytest.fixture()
def test_df(session, test_data, input_schema):
    return session.createDataFrame(data=test_data, schema=input_schema)

@pytest.fixture()
def output_df(session, test_data, output_schema):
    return session.createDataFrame(data=test_data, schema=output_schema)

def test_filtering(session, test_df, input_schema):
    expected_data = [
            (18, "rdrinanh@odnoklassniki.ru", "United Kingdom", "1ErM8yuF3ytzzxLy1uPvQuRveLBygxN15x", "china-unionpay"),
            (36, "dbuckthorpz@tmall.com", "Netherlands", "15X53Z9B9jUNrvFpbr7D554uSc5RL7Pnkg", "diners-club-international"),
            (62, "bbarham1p@wisc.edu", "Netherlands", "16qpYVt6YAAx4JYjzbA8SwTUHoTyB4twRF", "jcb")
    ]
    expected_df = session.createDataFrame(data=expected_data,
                                           schema=input_schema)
    filtered_df = filtering(test_df, 'United Kingdom,Netherlands')
    assert_df_equality(expected_df, filtered_df)

def test_rename_columns(df, df2):
    column_mapping = {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type"
    }
    renamed_df = rename_columns(test_df, column_mapping)
    assert_df_equality(renamed_df, output_df)
