from dbfunctions import *
import time

if __name__ == "__main__":
	# Create/connect to wrapped.db
	con = create_connection()
	while(True):
		# Compare navidrome.db to wrapped.db and update appropriate fields
		now = datetime.now(tz=ZoneInfo("America/New_York"))
		today = now.strftime('%Y-%m-%d')
		print(now.strftime('[%a, %B %d, %Y @ %H:%M:%S %Z] :\t ') + "Checking db for updates...\n")
		check_db_status(con)
		time.sleep(1)