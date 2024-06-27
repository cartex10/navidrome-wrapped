import sqlite3, os
from datetime import datetime
from zoneinfo import ZoneInfo
from enum import Enum
from pathlib import Path

wrappedDBPath = "db/wrapped.db"
wrappedLogPath = "db/wrapped.log"
navidromeDBPath = "navidrome/navidrome.db"

class mediaDBEnum(Enum):
	song_id = 0, 
	album_id = 1,
	artist_id = 2,
	path = 3,
	title = 4,
	album = 5,
	artist = 6,
	track_number = 7,
	created = 8,
	genre = 9

class typeEnum(Enum):
	song = 0,
	album = 1,
	artist = 2

def create_connection():
	# Create folder for database if it does not exist, then connect to/create db
	if not os.path.exists("db"):
		os.makedirs("db")
	con = sqlite3.connect(wrappedDBPath)
	# Check for tables with correct cells, create if they don't exist
	table_checks = ["SELECT song_id, user_id, play_count, starred FROM tracked_songs",
		"SELECT album_id, user_id, play_count, starred FROM tracked_albums",
		"SELECT artist_id, user_id, play_count, starred FROM tracked_artists",
		"SELECT song_id, album_id, artist_id, path, title, album, artist, track_number, created , genre FROM all_media",
		"SELECT song_id, album_id, artist_id, path, title, album, artist, track_number, created , genre FROM all_media_temp",
		"SELECT id, media_type, user_id, date, play_increase FROM media_plays" ]
	table_strings = ["CREATE TABLE tracked_songs (song_id TEXT PRIMARY KEY NOT NULL, user_id TEXT, play_count INT, starred BOOL);",
		"CREATE TABLE tracked_albums (album_id TEXT PRIMARY KEY NOT NULL, user_id TEXT, play_count INT, starred BOOL);",
		"CREATE TABLE tracked_artists (artist_id TEXT PRIMARY KEY NOT NULL, user_id TEXT, play_count INT, starred BOOL);",
		"CREATE TABLE all_media (song_id TEXT PRIMARY KEY NOT NULL, album_id TEXT, artist_id TEXT, path TEXT, title TEXT, album TEXT, artist TEXT, track_number INT, created DATE, genre TEXT);",
		"CREATE TABLE all_media_temp (song_id TEXT PRIMARY KEY NOT NULL, album_id TEXT, artist_id TEXT, path TEXT, title TEXT, album TEXT, artist TEXT, track_number INT, created DATE, genre TEXT);",
		"CREATE TABLE media_plays (id TEXT PRIMARY KEY NOT NULL, media_type TEXT, user_id TEXT, date DATE, play_increase INT);" ]
	for i in range(len(table_checks)):
		try:
			cur = con.execute(table_checks[i])
		except:
			cur = con.execute(table_strings[i])
	# Check for correct indexes, create if they don't exist
	#index
	#con.commit()
	return con

def navidrome_connection():
	# Connect to navidrome database
	return sqlite3.connect(navidromeDBPath)

def check_db_status(con):
	# If empty/new db, do initial full sync
	cur = con.execute("SELECT * from all_media")
	if len(cur.fetchall()) == 0:
		printBoth("No media metadata detected, starting full sync")
		full_db_sync(con)
	# Else, check for changes between wrapped db and navidrome db
	else:
		check_for_db_changes(con)
		check_for_media_plays(con)
		
def full_db_sync(con):
	media = read_mediafile_table()
	# Add all media file metadata to db
	for file in media:
		cur = con.execute("INSERT INTO all_media VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (file['song_id'], file['album_id'], file['artist_id'], file['path'], file['title'], file['album'], file['artist'], file['track_number'], file['created'], file['genre']) )
	media = read_annotations_table()
	# Add all annotation metadata to appropriate tables in db
	for file in media["albums"]:
		cur = con.execute("INSERT INTO tracked_albums VALUES (?, ?, ?, ?)", (file["album_id"], file["user_id"], file["play_count"], file["starred"]) )
	for file in media["songs"]:
		cur = con.execute("INSERT INTO tracked_songs VALUES (?, ?, ?, ?)", (file["song_id"], file["user_id"], file["play_count"], file["starred"]) )
	for file in media["artists"]:
		cur = con.execute("INSERT INTO tracked_artists VALUES (?, ?, ?, ?)", (file["artist_id"], file["user_id"], file["play_count"], file["starred"]) )
	con.commit()

def check_for_db_changes(con):
	# Copy navidrome media file db into temp media db
	media = read_mediafile_table()
	for file in media:
		cur = con.execute("INSERT INTO all_media_temp VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (file['song_id'], file['album_id'], file['artist_id'], file['path'], file['title'], file['album'], file['artist'], file['track_number'], file['created'], file['genre']) )
	con.commit()
	# Complete multiple sql inner joins to check for updated metadata from media_file (navidrome.db) in all_media (wrapped.db)
	joinClauses = [{"updated_value": "Genre", "sql": "o.genre != n.genre AND o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number AND o.path = n.path"},
	{"updated_value": "Track Number", "sql": "o.track_number != n.track_number AND o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.path = n.path"},
	{"updated_value": "Artist", "sql": "o.artist_id != n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number AND o.path = n.path"},
	{"updated_value": "Album", "sql": "o.album_id != n.album_id AND o.artist_id = n.artist_id AND o.track_number = n.track_number AND o.genre = n.genre"},
	{"updated_value": "Path", "sql": "o.path != n.path AND o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number"},
	{"updated_value": "ID Only", "sql": "o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number AND o.genre = n.genre AND o.path = n.path"} ]
	for jc in joinClauses:
		# joins tables with following conditions + A.song_id != B.song_id to check for updated song metadata
		cur = con.execute("SELECT o.song_id, n.song_id FROM all_media_temp n INNER JOIN all_media o ON o.title = n.title AND " + jc["sql"] + " AND o.song_id != n.song_id;")
		fetch = cur.fetchall()
		if len(fetch) > 0:
			for i in fetch:
				# Report metadata changes
				string = "Updated song metadata: " + jc["updated_value"] + " changed"
				string += "\n\t[" + i[0] + "] OLD\n" + stringMetadata(con, i[0])
				string += "\n\t[" + i[1] + "] NEW\n" + stringMetadata(con, i[1], 1)
				printBoth(string)
				meta = grabMetadata(con, i[1], 1)
				# Change relevant metadata fields in all_media
				if jc["updated_value"] == "Genre":
					# Updated genre (same title/artist/album/track_number/path)
					update_db(con, i[0], mediaDBEnum["genre"], meta["genre"])
				elif jc["updated_value"] == "Track Number":
					# Updated track number (same title/artist/album/path)
					update_db(con, i[0], mediaDBEnum["track_number"], meta["track_number"])
				elif jc["updated_value"] == "Artist":
					# Updated artist (same title/album/track_number/path)
					update_db(con, i[0], mediaDBEnum["artist_id"], meta["artist_id"])
					update_db(con, i[0], mediaDBEnum["artist"], meta["artist"])
				elif jc["updated_value"] == "Album":
					# Updated album/path (same title/artist/track_number/genre)
					update_db(con, i[0], mediaDBEnum["album_id"], meta["album_id"])
					update_db(con, i[0], mediaDBEnum["album"], meta["album"])
					update_db(con, i[0], mediaDBEnum["path"], meta["path"])
				elif jc["updated_value"] == "Path":
					# Updated path (same title/artist/album/track_number)
					update_db(con, i[0], mediaDBEnum["path"], meta["path"])
				#elif jc["updated_value"] == "ID Only":
					# Final check for updated id only (error correction) (same title/artist/album/track_number/genre/path)
				update_db(con, i[0], mediaDBEnum["song_id"], i[1])
	# Clear temp db
	cur = con.execute("DELETE FROM all_media_temp;")
	con.commit()

def check_for_media_plays(con):
	pass

def update_db(con, song_id, col, val):#, table=0):
	# Update one all_media table column, select by song_id, triggers should auto update fields in tracked_* tables
	cur = "UPDATE all_media SET "
	#if table:
	#	cur = "UPDATE all_media_temp SET "
	if col == mediaDBEnum["song_id"]:
		cur += "song_id=? WHERE song_id=?"
		sync_song_ID(con, song_id, val)
	elif col == mediaDBEnum["album_id"]:
		cur += "album_id=? WHERE song_id=?"
	elif col == mediaDBEnum["artist_id"]:
		cur += "artist_id=? WHERE song_id=?"
	elif col == mediaDBEnum["path"]:
		cur += "path=? WHERE song_id=?"
	elif col == mediaDBEnum["title"]:
		cur += "title=? WHERE song_id=?"
	elif col == mediaDBEnum["album"]:
		cur += "album=? WHERE song_id=?"
	elif col == mediaDBEnum["artist"]:
		cur += "artist=? WHERE song_id=?"
	elif col == mediaDBEnum["track_number"]:
		cur += "track_number=? WHERE song_id=?"
	elif col == mediaDBEnum["created"]:
		cur += "created=? WHERE song_id=?"
	elif col == mediaDBEnum["genre"]:
		cur += "genre=? WHERE song_id=?"
	cur = con.execute(cur, (val, song_id))
	con.commit()

def sync_song_ID(con, oldval, newval):
	# Sync tracked_songs table when song_id changes (should always run after changes to all_media song_id)
	cur = con.execute("SELECT song_id FROM tracked_songs WHERE song_id=?", (oldval,))
	if len(cur.fetchall()) > 0:
		cur = con.execute("UPDATE tracked_songs SET song_id=? WHERE song_id=?", (newval, oldval))
		con.commit()
		string = "Sync ID of song in tracked_songs table\n\t["
		string += oldval + "] OLD\n\n\t[" + newval + "] NEW\n\n"
		printBoth(string)

def read_mediafile_table():
	navidromeCon = navidrome_connection()
	cur = navidromeCon.execute("SELECT \
		id, \
		path, \
		title, \
		album, \
		artist, \
		artist_id, \
		track_number, \
		genre, \
		created_at, \
		album_id \
		FROM media_file")
	ret = []
	for file in cur.fetchall():
		ret2 = {"song_id": file[0], "path": file[1], "title": file[2], "album": file[3], "artist": file[4], "artist_id": file[5], "track_number": file[6], "genre": file[7], "album_id": file[9]}
		ret2["created"] = file[8].split('T')[0]
		ret.append(ret2)
	return ret

def read_annotations_table():
	navidromeCon = navidrome_connection()
	cur = navidromeCon.execute("SELECT \
		item_type,\
		item_id, \
		user_id, \
		play_count, \
		starred \
		FROM annotation")
	retAlbum = []
	retMediaFile = []
	retArtist = []
	for i in cur.fetchall():
		if i[0] == "album":
			retAlbum.append({"album_id": i[1], "user_id": i[2], "play_count": i[3], "starred": i[4]})
		elif i[0] == "media_file":
			retMediaFile.append({"song_id": i[1], "user_id": i[2], "play_count": i[3], "starred": i[4]})
		elif i[0] == "artist":
			retArtist.append({"artist_id": i[1], "user_id": i[2], "play_count": i[3], "starred": i[4]})
	return {"albums": retAlbum, "songs": retMediaFile, "artists": retArtist}

def grabMetadata(con, song_id, table=0):
	# Grab Metadata from all_media or temp
	if table == 0:
		cur = con.execute("SELECT song_id, album_id, artist_id, path, title, album, artist, track_number, created, genre FROM all_media WHERE song_id=?", (song_id,))
	elif table == 1:
		cur = con.execute("SELECT song_id, album_id, artist_id, path, title, album, artist, track_number, created, genre FROM all_media_temp WHERE song_id=?", (song_id,))
	fetch = cur.fetchall()[0]
	return {"song_id": fetch[0], "album_id": fetch[1], "artist_id": fetch[2], "path": fetch[3], "title": fetch[4], "album": fetch[5], "artist": fetch[6], "track_number": fetch[7], "created": fetch[8], "genre": fetch[9]}

def stringMetadata(con, song_id, table=0):
	meta = grabMetadata(con, song_id, table)
	string = "\t\tTitle: " + meta["title"]
	string += "\n\t\tArtist: " + meta["artist"]
	string += "\n\t\tAlbum: " + meta["album"]
	string += "\n\t\tTrack Number: " + str(meta["track_number"])
	string += "\n\t\tGenre: " + meta["genre"] + "\n"
	return string

def printToLog(string):
	with open(wrappedLogPath, "a") as file:
		file.write(string)

def printBoth(string):
	now = datetime.now(tz=ZoneInfo("America/New_York"))
	string = now.strftime('[%a, %B %d, %Y @ %H:%M:%S %Z] :\t ') + str(string) + "\n"
	printToLog(string)
	print(string)