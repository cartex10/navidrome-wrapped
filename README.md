# Navidrome-wrapped

## Python script that tracks changes in the navidrome database to keep track of plays even after admins change media metadata

## Running the program

### Docker container

Requires two folders:

- navidrome/ : same folder used by main navidrome service, should have `navidrome.db` in root

- db/: folder that stores this programs database, `wrapped.db`, and its log file, `wrapped.log`