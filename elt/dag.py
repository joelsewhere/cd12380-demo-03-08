from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import pathlib

DAG_ROOT = pathlib.Path(__file__).parent

ANALYTICS_CONN_ID = 'redshift_analytics'  # analytics database

@dag(
    schedule='@daily',
    start_date=datetime(2026, 3, 8),
    template_searchpath=[str(DAG_ROOT / 'sql')],
)
def quotes_analytics():

    author_geo_stats = SQLExecuteQueryOperator(
        task_id='author_geo_stats',
        conn_id=ANALYTICS_CONN_ID,
        sql='sql/author_geo_stats.sql',
    )

    tag_engagement = SQLExecuteQueryOperator(
        task_id='tag_engagement',
        conn_id=ANALYTICS_CONN_ID,
        sql='sql/tag_engagement.sql',
    )

    author_geo_stats, tag_engagement


quotes_analytics()