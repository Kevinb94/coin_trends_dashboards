chmod 777 ./postgres/log

# Postgres User creates this file, so we need to ensure it has the right permissions
# This is necessary to view the logs from the host
sudo chmod o+r postgresql.log
