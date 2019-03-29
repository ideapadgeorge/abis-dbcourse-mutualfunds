# Set-up WRDS connection
import wrds
db = wrds.Connection(wrds_username='joe')
db.create_pgpass_file()
db.close()
