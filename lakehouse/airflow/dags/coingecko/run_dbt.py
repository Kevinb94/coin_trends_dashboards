import subprocess

def run_dbt_transformations():
    subprocess.run(["dbt", "run", "--project-dir", "/dbt"], check=True)
