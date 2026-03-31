from airflow.sdk import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


@dag
def revenue_potential_model():
    
    missing_lead_scores = SQLExecuteQueryOperator(
        task_id="missing_lead_scores",
        conn_id="customers",
        sql="sql/missing_lead_scores.sql"
        )
    
    @task.branch
    def check_sql_output(output):
        
      print(output)

      if output:
          
          return 'trigger_lead_score_model'
      
      else:
          
          return 'generate_features'
        
    trigger_lead_score_model = TriggerDagRunOperator(
        task_id="trigger_lead_score_model",
        trigger_dag_id="lead_score_model",
        conf={"customer_ids": "{{ ti.xcom_pull(task_ids='missing_lead_scores') }}"} 
      )
    
    @task 
    def generate_features(query, storage_dir):
        
        import pathlib
        from airflow.providers.sqlite.hooks.sqlite import SqliteHook

        pathlib.Path(storage_dir).mkdir(parents=True, exist_ok=True)

        features_path = pathlib.Path(storage_dir) / 'features.csv'

        hook = SqliteHook(sqlite_conn_id="customers")

        df = hook.get_pandas_df(query)

        df.to_csv(features_path)

        return features_path.as_posix()
    
    @task 
    def run_model(storage_dir, features_path):
        
        import pandas as pd
        import pathlib

        df = pd.read_csv(features_path)

        predictions_path = pathlib.Path(storage_dir) / 'predictions.csv'

        predictions = df.assign(
            potential=df.cart_activity * df.lead_score
          )
        
        predictions.to_csv(predictions_path)

        return predictions_path.as_posix()
    
    @task
    def insert_results(predictions_path):
        
        import pandas as pd
        from airflow.providers.sqlite.hooks.sqlite import SqliteHook

        df = pd.read_csv(predictions_path)

        hook = SqliteHook(sqlite_conn_id="customers")

        hook.run("""
            CREATE TABLE IF NOT EXISTS revenue_potential (
                customer_id  INTEGER,
                potential    FLOAT,
                );
            """)
        
        df.to_sql('revenue_potential', con=hook.get_sqlalchemy_engine(), index=False)

    import pathlib
    query = pathlib.Path(__file__).parent / 'sql' / 'revenue_potential_features.sql'
    storage_dir = '/workspace/external_storage/{{ dag.dag_id }}/{{ dag_run.run_id }}'
    branch = check_sql_output(missing_lead_scores)
    features = generate_features(query, storage_dir)
    branch >> [trigger_lead_score_model, features]
    predictions = run_model(storage_dir, features)
    insert_results(predictions)

revenue_potential_model()

