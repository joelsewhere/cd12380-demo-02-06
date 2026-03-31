from airflow.sdk import dag, task_group
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag(max_active_tasks=1)
def init_database():

    @task_group
    def drop():

        for table in [
            'customer',
            'web_activity',
            'cart_activity',
            'lead_score',
            ]:

            SQLExecuteQueryOperator(
                task_id="customer",
                conn_id="customers",
                sql=f"DROP TABLE IF EXISTS {table};"
                )

    @task_group
    def create():

        SQLExecuteQueryOperator(
            task_id="customer",
            conn_id="customers",
            sql="""
            CREATE TABLE IF NOT EXISTS customer (
                id INTEGER
                )  
            """
            )
        
        SQLExecuteQueryOperator(
            task_id="web_activity",
            conn_id="customers",
            sql="""
            CREATE TABLE IF NOT EXISTS web_activity (
                customer_id INTEGER,
                activity    INTEGER
                )  
            """
            )
        
        SQLExecuteQueryOperator(
            task_id="cart_activity",
            conn_id="customers",
            sql="""
            CREATE TABLE IF NOT EXISTS cart_activity (
                customer_id INTEGER,
                activity    INTEGER
                )  
            """
            )

        SQLExecuteQueryOperator(
            task_id="lead_score",
            conn_id="customers",
            sql="""
            CREATE TABLE IF NOT EXISTS lead_score (
                customer_id INTEGER,
                score       FLOAT
                )  
            """
            )
    
    @task_group
    def seed():

        SQLExecuteQueryOperator(
            task_id="customers",
            conn_id="customers",
            sql="""
            INSERT INTO customer (id)
            VALUES 
                (1),
                (2),
                (3);
            """
            )
        
        SQLExecuteQueryOperator(
            task_id="web_activity",
            conn_id="customers",
            sql="""
            INSERT INTO web_activity (customer_id, activity)
            VALUES 
                (1, 10),
                (2, 1),
                (3, 20);
            """
            )
        
        SQLExecuteQueryOperator(
            task_id="cart_activity",
            conn_id="customers",
            sql="""
            INSERT INTO cart_activity (customer_id, activity)
            VALUES 
                (1, 3),
                (2, 2),
                (3, 5);
            """
            )
        
        SQLExecuteQueryOperator(
            task_id="lead_score",
            conn_id="customers",
            sql="""
            INSERT INTO customer (customer_id, score)
            VALUES 
                (1, 1.5),
                (2, .2);
            """
            )
    drop() >> create() >> seed()

init_database()
        

        

