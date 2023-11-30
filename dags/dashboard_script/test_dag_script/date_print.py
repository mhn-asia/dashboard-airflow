from datetime import datetime, timedelta


def my_python_function(**kwargs):
    execution_date = kwargs['execution_date']
    previous_date = execution_date - timedelta(days=1)
    formatted_date = previous_date.strftime("%Y-%m-%d")
    print(f"The script is running for execution date: {formatted_date}")
    # Your script logic here, using the execution_date as needed