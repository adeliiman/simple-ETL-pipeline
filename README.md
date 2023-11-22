# simple-ETL-pipeline
data pipeline to extract data from TSETMC
load data in postgreSQL

set environment variable in $HOME/.bash_profile
export postgres_host = ''
export postgres_database = ''
export postgres_user = ''
export postgres_password = ''
export postgres_port = ''

# crontab

crontab -e

add:
* * * * * /path/to/script.sh >> ~/backup.log 2>&1

