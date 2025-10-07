# ml_forecasting_dag.py

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta, datetime

def return_snowflake_conn():
    """Initialize the SnowflakeHook and return a cursor"""
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    conn = hook.get_conn()
    return conn.cursor()

@task
def create_schemas(cur):
    """Create necessary schemas if they don't exist"""
    try:
        cur.execute("CREATE SCHEMA IF NOT EXISTS RAW;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ADHOC;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS;")
        print("Schemas created successfully")
    except Exception as e:
        print(e)
        raise

@task
def train(cur, train_input_table, train_view, forecast_function_name):
    """
    - Create a view with training related columns
    - Create a model with the view above
    """

    create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
        DATE, CLOSE, SYMBOL
        FROM {train_input_table};"""

    create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
        INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
        SERIES_COLNAME => 'SYMBOL',
        TIMESTAMP_COLNAME => 'DATE',
        TARGET_COLNAME => 'CLOSE',
        CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
    );"""

    try:
        cur.execute(create_view_sql)
        cur.execute(create_model_sql)
        # Inspect the accuracy metrics of your model
        cur.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        print("Training completed successfully")
    except Exception as e:
        print(e)
        raise

@task
def predict(cur, forecast_function_name, train_input_table, forecast_table, final_table):
    """
    - Generate predictions and store the results to a table named forecast_table
    - Union your predictions with your historical data, then create the final table
    """

    make_prediction_sql = f"""BEGIN
        -- This is the step that creates your predictions
        CALL {forecast_function_name}!FORECAST(
            FORECASTING_PERIODS => 7,
            -- Here we set your prediction interval
            CONFIG_OBJECT => {{'prediction_interval': 0.95}}
        );
        -- These steps store your predictions to a table
        LET x := SQLID;
        CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
    END;"""

    create_final_table_sql = f"""
        CREATE OR REPLACE TABLE {final_table} AS
        SELECT *
        FROM (
        -- Historical actuals
        SELECT
            SYMBOL,
            DATE::DATE            AS DATE,
            CLOSE                 AS actual,
            NULL::FLOAT           AS forecast,
            NULL::FLOAT           AS lower_bound,
            NULL::FLOAT           AS upper_bound
        FROM {train_input_table}

        UNION ALL

        -- Forecast rows
        SELECT
            REPLACE(series, '', '') AS SYMBOL,
            CAST(ts AS DATE)        AS DATE,       -- drop timestamp part
            NULL::FLOAT             AS actual,
            forecast,
            lower_bound,
            upper_bound
        FROM {forecast_table}
        )
        ORDER BY DATE DESC;"""

    try:
        cur.execute(make_prediction_sql)
        cur.execute(create_final_table_sql)
        print("Prediction and final table created successfully")
    except Exception as e:
        print(e)
        raise

with DAG(
    dag_id='ml_forecasting_pipeline',
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['ML', 'Forecasting', 'ELT'],
    schedule="30 3 * * *",  # Run at 3:30 AM (after yfinance ETL completes)
    default_args={
        'owner': 'calvin/saketh',
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }
) as dag:

    # Table and view names
    train_input_table = "RAW.LAB1_STOCK_INFO"
    train_view = "adhoc.market_data_view"
    forecast_table = "adhoc.market_data_forecast"
    forecast_function_name = "ANALYTICS.lab1_predict_stock_price"
    final_table = "ANALYTICS.lab1_final_table"

    # Get Snowflake cursor
    cur = return_snowflake_conn()

    # Define tasks
    schema_task = create_schemas(cur)
    train_task = train(cur, train_input_table, train_view, forecast_function_name)
    predict_task = predict(cur, forecast_function_name, train_input_table, forecast_table, final_table)

    # Set task dependencies
    schema_task >> train_task >> predict_task
