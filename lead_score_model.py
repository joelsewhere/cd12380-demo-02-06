from airflow.sdk import dag, task, Param

@dag(
    params=Param(None, dtype=['array', 'null'])
)
def lead_score_model():

    @task
    def generate_features(query, storage_dir):

        import pathlib
        from airflow.providers.sqlite.hooks.sqlite import SqliteHook

        hook = SqliteHook(sqlite_conn_id="customers")

        features = hook.get_pandas_df(query)

        pathlib.Path(storage_dir).mkdir(parents=True, exist_ok=True)

        features_path = pathlib.Path(storage_dir) / 'features.csv'

        features.to_csv(features_path)

        return features_path.as_posix()
    
    @task
    def run_model(storage_dir, features_path):

        import pathlib
        import numpy as np
        import pandas as pd

        df = pd.read_csv(features_path)

        predictions = df.assign(score=np.log(df.web_activity))[['customer_id', 'score']]

        predictions_path = pathlib.Path(storage_dir) / 'predictions.csv'

        predictions.to_csv(predictions_path)

        return predictions_path.as_posix()
    
    @task
    def insert_records(predictions_path):

        import pandas as pd
        from airflow.providers.sqlite.hooks.sqlite import SqliteHook

        hook = SqliteHook(sqlite_conn_id="customers")

        df = pd.read_csv(predictions_path)

        df.to_sql('lead_score', con=hook.get_sqlalchemy_engine(), index=False)

    
    import pathlib

    query = pathlib.Path(__file__).parent / 'sql' / 'lead_score_features.sql'
    storage_dir = "/workspace/external_storage/{{ dag.dag_id }}/{{ dag_run.run_id }}"

    features = generate_features(query, storage_dir)
    predictions = run_model(storage_dir, features)
    insert_records(predictions)














    


