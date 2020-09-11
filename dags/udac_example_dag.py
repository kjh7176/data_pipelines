from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'catchup': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          max_active_runs = 1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = DummyOperator(task_id='Stage_events',  dag=dag)
"""
StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_buckt="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json",
    table="staging_events"    
)
"""
stage_songs_to_redshift = DummyOperator(task_id='Stage_songs',  dag=dag)
"""
StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_buckt="udacity-dend",
    s3_key="song_data",
    json_path="auto",
    table="staging_songs"    
)
"""
load_songplays_table = DummyOperator(task_id='Load_songplays_fact_table',  dag=dag)
"""LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays"
)
"""
load_user_dimension_table = DummyOperator(task_id='Load_user_dim_table',  dag=dag)
"""LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="users",
    append_only=False
)
"""
load_song_dimension_table = DummyOperator(task_id='Load_song_dim_table',  dag=dag)
"""LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songs",
    append_only=False
)
"""
load_artist_dimension_table = DummyOperator(task_id='Load_artist_dim_table',  dag=dag)
"""LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="artists",
    append_only=False
)
"""
load_time_dimension_table = DummyOperator(task_id='Load_time_dim_table',  dag=dag)
"""LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="time",
    append_only=False
)
"""
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    redshift_conn_id="redshift",
    params={
        "table": ["songplays", "songs", "users", "artists", "time"]
    }
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift >> load_songplays_table 
start_operator >> stage_songs_to_redshift >> load_songplays_table 
load_songplays_table >> load_user_dimension_table >> run_quality_checks
load_songplays_table >> load_song_dimension_table >> run_quality_checks
load_songplays_table >> load_artist_dimension_table >> run_quality_checks
load_songplays_table >> load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator