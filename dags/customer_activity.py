from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

def create_customer_activity_table():
    hook = PostgresHook(postgres_conn_id="postgres_dw")
    connection = hook.get_conn()
    cursor = connection.cursor()
    
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS customer_activity (
        activity_id SERIAL PRIMARY KEY,
        customer_id INTEGER,
        order_id INTEGER,
        login_attempt_id INTEGER,
        activity_date TIMESTAMP,
        activity_type VARCHAR(50)
    );
    """
    
    insert_data_sql = """
    INSERT INTO customer_activity (customer_id, order_id, login_attempt_id, activity_date, activity_type)
    SELECT 
        c.id AS customer_id,
        o.id AS order_id,
        l.id AS login_attempt_id,
        CASE 
            WHEN o.created_at IS NOT NULL THEN o.created_at
            ELSE l.timestamp
        END AS activity_date,
        CASE 
            WHEN o.id IS NOT NULL THEN 'Order'
            ELSE 'Login Attempt'
        END AS activity_type
    FROM 
        customers c
    LEFT JOIN 
        orders o ON c.id = o.customer_id
    LEFT JOIN 
        login_attempts l ON c.id = l.customer_id;
    """
    
    cursor.execute(create_table_sql)
    cursor.execute(insert_data_sql)
    connection.commit()
    cursor.close()
    connection.close()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
}

dag = DAG(
    "create_customer_activity_table",
    default_args=default_args,
    description="Create Customer Activity Table",
    schedule_interval="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

create_customer_activity_task = PythonOperator(
    task_id="create_customer_activity_table",
    python_callable=create_customer_activity_table,
    dag=dag,
)