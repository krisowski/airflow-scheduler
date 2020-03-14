from datetime import datetime
import pytest
import pytz

from dags.scheduler import get_previous_execution_date

@pytest.mark.parametrize(
    'other_dag_cron_schedule,this_dag_cron_schedule,current_execution_date,expected_date',
    [
        ('0 */2 * * *', '0 10 * * *', '2020-03-06 10:00:00', '2020-03-07 08:00:00'),
        ('0 */2 * * *', '0 11 * * *', '2020-03-06 11:00:00', '2020-03-07 08:00:00'),
    ]
)
def test_get_previous_execution_date(other_dag_cron_schedule, this_dag_cron_schedule, current_execution_date,
                                     expected_date):
    expected_date = datetime.strptime(expected_date, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
    prev_date = get_previous_execution_date(other_dag_cron_schedule, this_dag_cron_schedule,
                                            datetime.strptime(current_execution_date, '%Y-%m-%d %H:%M:%S'))
    assert prev_date == expected_date
