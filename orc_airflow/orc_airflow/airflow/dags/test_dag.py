from airflow.sdk import task, dag


@dag(dag_id="test_dag", description="a test dag to check if airflow itself is working")
def test_dag():
    @task
    def test_task():
        return "Hello World"

    test_task()


test_dag()
