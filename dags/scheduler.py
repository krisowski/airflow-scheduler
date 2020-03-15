from datetime import datetime, timedelta
from croniter import croniter
from functools import partial
from utils import convert_pendulum_object_to_datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 3, 1),
    'retries': 20,
    'retry_delay': timedelta(minutes=10),
}

DAG1_SCHEDULE = "0 */2 * * *" # every two hours, i.e. 0, 2, 4, etc.
DAG2_SCHEDULE = "0 11 * * *"  # once a day at 11:00


def dag_info(**kwargs):
    print("execution_date:      {}".format(kwargs.get('execution_date')))
    print("prev_execution_date: {}".format(kwargs.get('prev_execution_date')))
    print("next_execution_date: {}".format(kwargs.get('next_execution_date')))


def get_previous_execution_date(other_dag_schedule, this_dag_schedule, current_execution_date, **kwargs):
    print(f"current_execution_date: {current_execution_date}")

    current_execution_date = convert_pendulum_object_to_datetime(current_execution_date)
    this_dag_schedule = croniter(this_dag_schedule, current_execution_date, ret_type=datetime)

    # jump to the next schedule time when the DAG is really executed
    this_dag_real_execution_time = this_dag_schedule.get_next()

    # use that time to initialize the other DAG schedule
    other_cron_schedule = croniter(other_dag_schedule, this_dag_real_execution_time , ret_type=datetime)

    # move one schedule period forwards and backwards to handle the cases when trigger time of both dags is the same
    # or it is different
    other_cron_schedule.get_next()
    other_cron_schedule.get_prev()

    # get the previous DAG schedule
    return other_cron_schedule.get_prev()


with DAG('dag_1', default_args=default_args, schedule_interval=DAG1_SCHEDULE, catchup=False) as dag1:
    task1_dag1 = PythonOperator(
        task_id='task1_1',
        provide_context=True,
        python_callable=dag_info
    )

with DAG('dag_2', default_args=default_args, schedule_interval=DAG2_SCHEDULE, catchup=False) as dag2:
    sensor_dag2 = ExternalTaskSensor(
        task_id="sensor",
        external_dag_id=dag1.dag_id,
        external_task_id=task1_dag1.task_id,
        execution_date_fn = partial(get_previous_execution_date, dag1.schedule_interval, dag2.schedule_interval)
    )

    task1_dag2 = PythonOperator(
        task_id='task1_2',
        provide_context=True,
        python_callable=dag_info
    )

    sensor_dag2 >> task1_dag2

