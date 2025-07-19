docker compose run --rm airflow-cli airflow dags unpause test_pipeline_simulation

docker compose run --rm airflow-cli airflow dags trigger test_pipeline_simulation

# Check status
docker compose run --rm airflow-cli airflow tasks list test_pipeline_simulation
docker compose run --rm airflow-cli airflow tasks logs test_pipeline_simulation fetch_data


# Run cli interactively
docker compose run --rm -it airflow-cli /bin/bash

# After interactively getting into container
airflow tasks logs test_pipeline_simulation fetch_data

