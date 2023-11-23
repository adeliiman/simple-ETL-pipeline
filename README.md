# simple-ETL-pipeline
## Building a Data Pipeline to Retrieve Data from TSETMC and Store in PostgreSQL using Crontab


#### set environment variable in  $HOME/.bash_profile
> export postgres_host = 127.0.0.1 <br>
> export postgres_database = db <br>
> export postgres_user = user <br>
> export postgres_password = pass <br>
> export postgres_port = 5432 <br>

#### crontab

> crontab -e

add:
> \* * * * * /path/to/script.sh >> ~/backup.log 2>&1

