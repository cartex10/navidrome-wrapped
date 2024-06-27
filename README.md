# Navidrome-wrapped

## Python script that tracks changes in the navidrome database to keep track of plays even after admins change media metadata

## Current Functions

- Simple tracking of media file ids to keep media plays across file changes
- Seamlessly attaches to active navidrome database to store changes in metadata
- Stores changes in plays daily in simple sqlite3 database, to allow for data visualization
- Does not write to navidrome database, no risk of corruption

## Running the program

### Docker container

Requires Docker, use docker compose and the provided file to set up the container automatically.

Requires two folders:

- navidrome/ : same folder used by main navidrome service, should have `navidrome.db` in root

- db/: folder that stores this programs database, `wrapped.db`, and its log file, `wrapped.log`

### Python

No external libraries should be necessary