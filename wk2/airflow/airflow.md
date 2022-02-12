# Apache Airflow

## Installation and Start up

### Downloading `docker-compose.yaml`

```bash
$ curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.3/docker-compose.yaml'
```
There are three mounted directories
1. `./dags` - put your DAG files here
2. `./logs` - contains logs from task execution and scheduler
3. `./plugins` - you can put your custom plugins here.

For MacOS, we need to change the docker resources setting to > 4GB memory.

### Initializing the database
```bash
$ docker-compose up airflow-init
```
The account created has the login `airflow` and the password `airflow`.

### Cleaning up the environment
```bash
$ docker-compose down --volumes --remove-orphans
```

### Running airflow
```bash
$ docker-compose up
```

### Accessing the environment
After starting Airflow, you can interact with it in 3 ways
1. By running CLI commands.
2. Via a browser using the web interface.
3. Using the REST API.

### Shutting down and cleaning up
``` bash
$ docker-compose down --volumes --rmi all
```

## DAG Files

### Importing Modules
```python
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
```

### Default Arguments
``` python
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
```
More information on [here]("https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/index.html#airflow.models.BaseOperator")

### Instantiate a DAG
We’ll need a DAG object to nest our tasks into. Here we pass a string that defines the dag_id, which serves as a unique identifier for your DAG. We also pass the default argument dictionary that we just defined and define a schedule_interval of 1 day for the DAG.
```python
with DAG(
    'tutorial', # dag_id (unique identifier for DAG)
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
```

### Tasks
Tasks are generated when instantiating operator objects. An object instantiated from an operator is called a task. The first argument task_id acts as a unique identifier for the task.
```python
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
)

t2 = BashOperator(
    task_id='sleep',
    depends_on_past=False,
    bash_command='sleep 5',
    retries=3,
)
```

### Templating with Jinja
The pipeline author can use built-in parameters and macros using Jinja.
```python
templated_command = dedent(
    """
{% for i in range(5) %}
    echo "{{ ds }}"
    echo "{{ macros.ds_add(ds, 7)}}"
    echo "{{ params.my_param }}"
{% endfor %}
"""
)

t3 = BashOperator(
    task_id='templated',
    depends_on_past=False,
    bash_command=templated_command,
    params={'my_param': 'Parameter I passed in'},
)
```
`{{ ds }}` is today's datestamp

### Documentation
``` python
t1.doc_md = dedent(
    """\
#### Task Documentation
You can document your task using the attributes `doc_md` (markdown),
`doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
rendered in the UI's Task Instance Details page.
![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)

"""
)

dag.doc_md = __doc__  # providing that you have a docstring at the beginning of the DAG
dag.doc_md = """
This is a documentation placed anywhere
"""  # otherwise, type it like this

```

### Setting up dependency
```python
t1.set_downstream(t2)

# This means that t2 will depend on t1
# running successfully to run.
# It is equivalent to:
t2.set_upstream(t1)

# The bit shift operator can also be
# used to chain operations:
t1 >> t2

# And the upstream dependency with the
# bit shift operator:
t2 << t1

# Chaining multiple dependencies becomes
# concise with the bit shift operator:
t1 >> t2 >> t3

# A list of tasks can also be set as
# dependencies. These operations
# all have the same effect:
t1.set_downstream([t2, t3])
t1 >> [t2, t3]
[t2, t3] << t1
```