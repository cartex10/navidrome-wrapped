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
		"SELECT album_id, artist_id, path, title, album, artist, track_number, created, genre FROM all_media",
		"SELECT media_id, media_type, user_id, date, play_increase FROM media_plays" ]
	table_strings = ["CREATE TABLE tracked_songs (song_id TEXT, user_id TEXT, play_count INT, starred BOOL);",
		"CREATE TABLE tracked_albums (album_id TEXT, user_id TEXT, play_count INT, starred BOOL);",
		"CREATE TABLE tracked_artists (artist_id TEXT, user_id TEXT, play_count INT, starred BOOL);",
		"CREATE TABLE all_media (song_id TEXT PRIMARY KEY, album_id TEXT, artist_id TEXT, path TEXT, title TEXT, album TEXT, artist TEXT, track_number INT, created DATE, genre TEXT);",
		"CREATE TABLE media_plays (media_id TEXT, media_type TEXT, user_id TEXT, date DATE, play_increase INT);" ]
	for i in range(len(table_checks)):
		try:
			cur = con.execute(table_checks[i])
		except:
			cur = con.execute(table_strings[i])
	# Attach to Navidrome db
	cur = con.execute("ATTACH DATABASE '" + navidromeDBPath + "' AS navidromeDB;")
	# Check for correct indexes, create if they don't exist
	#index
	#con.commit()
	return con

def check_db_status(con):
	# If empty/new db, do initial full sync
	cur = con.execute("SELECT * from all_media")
	if len(cur.fetchall()) == 0:
		printBoth("No media metadata detected, starting full sync\n\n")
		full_db_sync(con)
	# Else, check for changes between wrapped db and navidrome db
	else:
		check_for_db_changes(con)
		check_for_media_plays(con)
		
def full_db_sync(con):
	media = read_mediafile_table(con)
	# Add all media file metadata to db
	for file in media:
		insert_media_entry(con, file['song_id'], \
			file['album_id'], \
			file['artist_id'], \
			file['path'], \
			file['title'], \
			file['album'], \
			file['artist'], \
			file['track_number'], \
			file['created'], \
			file['genre'])
	media = read_annotations_table(con)
	# Add all annotation metadata to appropriate tables in db
	for file in media["albums"]:
		cur = con.execute("INSERT INTO tracked_albums \
			(album_id, user_id, play_count, starred) VALUES \
			(?, ?, ?, ?)", \
			(file["album_id"], \
			file["user_id"], \
			file["play_count"], \
			file["starred"]) )
	for file in media["songs"]:
		cur = con.execute("INSERT INTO tracked_songs \
			(song_id, user_id, play_count, starred) VALUES \
			(?, ?, ?, ?)", \
			(file["song_id"], \
			file["user_id"], \
			file["play_count"], \
			file["starred"]) )
	for file in media["artists"]:
		cur = con.execute("INSERT INTO tracked_artists \
			(artist_id, user_id, play_count, starred) VALUES \
			(?, ?, ?, ?)", \
			(file["artist_id"], \
			file["user_id"], \
			file["play_count"], \
			file["starred"]) )
	con.commit()

def check_for_db_changes(con):
	# Complete multiple sql inner joins to check for updated metadata from media_file (navidrome.db) in all_media (wrapped.db)
	joinClauses = [{"updated_value": "Genre", "sql": "o.genre != n.genre AND o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number AND o.path = n.path"},
	{"updated_value": "Track Number", "sql": "o.track_number != n.track_number AND o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.path = n.path"},
	{"updated_value": "Artist", "sql": "o.artist_id != n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number AND o.path = n.path"},
	{"updated_value": "Album", "sql": "o.album_id != n.album_id AND o.artist_id = n.artist_id AND o.track_number = n.track_number AND o.genre = n.genre"},
	{"updated_value": "Path", "sql": "o.path != n.path AND o.artist_id = n.artist_id AND o.album_id = n.album_id AND o.track_number = n.track_number"} ]
	# ADD TITLE ?
	for jc in joinClauses:
		# joins tables with following conditions + A.song_id != B.song_id to check for updated song metadata
		cur = con.execute("SELECT o.song_id, n.id FROM navidromeDB.media_file n INNER JOIN all_media o ON o.title = n.title AND " + jc["sql"] + ";")
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
				update_db(con, i[0], mediaDBEnum["song_id"], i[1])
	# Check for new media files
	cur = con.execute("SELECT n.id, o.song_id, n.album_id, n.artist_id, n.path, n.title, n.album, n.artist, n.track_number, n.created_at, n.genre FROM navidromeDB.media_file n LEFT JOIN all_media o ON o.album_id = n.album_id AND o.title = n.title AND o.artist_id = n.artist_id AND o.path = n.path AND o.track_number = n.track_number")
	count = 0
	for i in cur.fetchall():
		try:
			if i[1] == None:
				insert_media_entry(con, i[0], i[2], i[3], i[4], i[5], i[6], i[7], i[8], i[9].split('T')[0], i[10])
				count += 1
		except:
			printBoth("Failed to add new song to all_media\n" + stringMetadata(con, i[0], 1))
	if count > 0:
		printBoth(str(count) + " new media file(s) found.\n")
	
def check_for_media_plays(con):
	# Joins navidrome annotations table and wrapped tracked_* tables, adds new entries to appropriate tables 
	# Updates play counts of media_files/songs in wrapped tracked_songs table
	insertCount = 0
	updateCount = 0
	cur = con.execute("SELECT n.item_id, n.user_id, o.play_count, n.play_count, n.starred FROM navidromeDB.annotation n LEFT JOIN tracked_songs o ON (o.song_id = n.item_id AND o.user_id = n.user_id) WHERE n.item_type='media_file'")
	fetch = cur.fetchall()
	doPrint = False
	for i in fetch:
		if i[2] == None:
			insert_play_count(con, i[0], i[1], typeEnum["song"], i[3], i[4])
			insertCount += 1
		else:
			if i[2] == i[3]:
				continue
			elif i[2] < i[3]:
				# If new plays have been registered
				update_play_increase(con, i[0], i[1], typeEnum["song"], i[3] - i[2])
			elif i[2] > i[3]:
				# If plays have been reset, assuming not lowered
				update_play_increase(con, i[0], i[1], typeEnum["song"], i[3])
			update_play_count(con, i[0], i[1], typeEnum["song"], i[3], i[4])
			updateCount += 1
			doPrint = True
	if doPrint:
		printBoth("Added " + str(insertCount) + " new media file user data entry(s).\n")
		printBoth("Updated user data of " + str(updateCount) + " media file(s).\n")
	# albums
	insertCount = 0
	updateCount = 0
	cur = con.execute("SELECT n.item_id, n.user_id, o.play_count, n.play_count, n.starred FROM navidromeDB.annotation n LEFT JOIN tracked_albums o ON (o.album_id = n.item_id AND o.user_id = n.user_id) WHERE n.item_type='album'")
	fetch = cur.fetchall()
	doPrint = False
	for i in fetch:
		if i[2] == None:
			insert_play_count(con, i[0], i[1], typeEnum["album"], i[3], i[4])
			insertCount += 1
		else:
			if i[2] == i[3]:
				continue
			elif i[2] < i[3]:
				# If new plays have been registered
				update_play_increase(con, i[0], i[1], typeEnum["album"], i[3] - i[2])
			elif i[2] > i[3]:
				# If plays have been reset, assuming not lowered
				update_play_increase(con, i[0], i[1], typeEnum["album"], i[3])
			update_play_count(con, i[0], i[1], typeEnum["album"], i[3], i[4])
			updateCount += 1
			doPrint = True
	if doPrint:
		printBoth("Added " + str(insertCount) + " new album user data entry(s).\n")
		printBoth("Updated user data of " + str(updateCount) + " album(s).\n")
	# artists
	insertCount = 0
	updateCount = 0
	cur = con.execute("SELECT n.item_id, n.user_id, o.play_count, n.play_count, n.starred FROM navidromeDB.annotation n LEFT JOIN tracked_artists o ON (o.artist_id = n.item_id AND o.user_id = n.user_id) WHERE n.item_type='artist'")
	fetch = cur.fetchall()
	doPrint = False
	for i in fetch:
		if i[2] == None:
			insert_play_count(con, i[0], i[1], typeEnum["artist"], i[3], i[4])
			insertCount += 1
		else:
			if i[2] == i[3]:
				continue
			elif i[2] < i[3]:
				# If new plays have been registered
				update_play_increase(con, i[0], i[1], typeEnum["artist"], i[3] - i[2])
			elif i[2] > i[3]:
				# If plays have been reset, assuming not lowered
				update_play_increase(con, i[0], i[1], typeEnum["artist"], i[3])
			update_play_count(con, i[0], i[1], typeEnum["artist"], i[3], i[4])
			updateCount += 1
			doPrint = True
	if doPrint:
		printBoth("Added " + str(insertCount) + " new artist user data entry(s).\n")
		printBoth("Updated user data of " + str(updateCount) + " artist(s).\n")

def insert_play_count(con, media_id, user_id, media_type, play_count, starred):
	now = datetime.now(tz=ZoneInfo("America/New_York"))
	today = now.strftime("%Y-%m-%d")
	if media_type == typeEnum["song"]:
		cur = con.execute("INSERT INTO tracked_songs (song_id, user_id, play_count, starred) VALUES (?, ?, ?, ?)", (media_id, user_id, play_count, starred))
	elif media_type == typeEnum["album"]:
		cur = con.execute("INSERT INTO tracked_albums (album_id, user_id, play_count, starred) VALUES (?, ?, ?, ?)", (media_id, user_id, play_count, starred))
	elif media_type == typeEnum["artist"]:
		cur = con.execute("INSERT INTO tracked_artists (artist_id, user_id, play_count, starred) VALUES (?, ?, ?, ?)", (media_id, user_id, play_count, starred))
	con.commit()

def update_play_count(con, media_id, user_id, media_type, newval, starred):
	if media_type == typeEnum["song"]:
		cur = con.execute("UPDATE tracked_songs SET play_count=? WHERE song_id=? AND user_id=?", (newval, media_id, user_id))
		cur = con.execute("UPDATE tracked_songs SET starred=? WHERE song_id=? AND user_id=?", (starred, media_id, user_id))
	elif media_type == typeEnum["album"]:
		cur = con.execute("UPDATE tracked_albums SET play_count=? WHERE album_id=? AND user_id=?", (newval, media_id, user_id))
		cur = con.execute("UPDATE tracked_albums SET starred=? WHERE album_id=? AND user_id=?", (starred, media_id, user_id))
	elif media_type == typeEnum["artist"]:
		cur = con.execute("UPDATE tracked_artists SET play_count=? WHERE artist_id=? AND user_id=?", (newval, media_id, user_id))
		cur = con.execute("UPDATE tracked_artists SET starred=? WHERE artist_id=? AND user_id=?", (starred, media_id, user_id))
	con.commit()

def update_play_increase(con, media_id, user_id, media_type, newval):
	now = datetime.now(tz=ZoneInfo("America/New_York"))
	today = now.strftime("%Y-%m-%d")
	if media_type == typeEnum["song"]:
		cur = con.execute("SELECT play_increase FROM media_plays WHERE media_id=? AND user_id=? AND date=?", (media_id, user_id, today))
		fetch = cur.fetchall()
		if len(fetch) == 1:
			inc = fetch[0][0] + newval
			cur = con.execute("UPDATE media_plays SET play_increase=? WHERE media_id=? AND user_id=? AND date=?", (inc, media_id, user_id, today))
		elif len(fetch) == 0:
			cur = con.execute("INSERT INTO media_plays \
				(media_id, media_type, user_id, date, play_increase) VALUES (?, 'media_file', ?, ?, ?)", (media_id, user_id, today, newval))
		else:
			printBoth("Too many ")
	elif media_type == typeEnum["album"]:
		cur = con.execute("SELECT play_increase FROM media_plays WHERE media_id=? AND user_id=? AND date=? AND media_type='album'", (media_id, user_id, today))
		fetch = cur.fetchall()
		if len(fetch) == 1:
			inc = fetch[0][0] + newval
			cur = con.execute("UPDATE media_plays SET play_increase=? WHERE media_id=? AND user_id=? AND date=? AND media_type='album'", (inc, media_id, user_id, today))
		elif len(fetch) == 0:
			cur = con.execute("INSERT INTO media_plays \
				(media_id, media_type, user_id, date, play_increase) VALUES (?, 'album', ?, ?, ?)", (media_id, user_id, today, newval))
		else:
			printBoth("Too many ")
		#cur = con.execute("UPDATE media_plays SET play_increase=? WHERE id=? AND user_id=? AND date=?")
	con.commit()

def insert_media_entry(con, song_id, album_id, artist_id, path, title, album, artist, track_number, created, genre):
	cur = con.execute("INSERT INTO all_media \
		(song_id, album_id, artist_id, path, title, album, artist, track_number, created, genre) VALUES \
		(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", \
		(song_id, \
		album_id, \
		artist_id, \
		path, \
		title, \
		album, \
		artist, \
		track_number, \
		created, \
		genre) )
	con.commit()

def update_db(con, song_id, col, val):
	# Update one all_media table column, select by song_id
	cur = "UPDATE all_media SET "
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

def read_mediafile_table(con):
	cur = con.execute("SELECT \
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
		FROM navidromeDB.media_file")
	ret = []
	for file in cur.fetchall():
		ret2 = {"song_id": file[0], "path": file[1], "title": file[2], "album": file[3], "artist": file[4], "artist_id": file[5], "track_number": file[6], "genre": file[7], "album_id": file[9]}
		ret2["created"] = file[8].split('T')[0]
		ret.append(ret2)
	return ret

def read_annotations_table(con):
	cur = con.execute("SELECT \
		item_type,\
		item_id, \
		user_id, \
		play_count, \
		starred \
		FROM navidromeDB.annotation")
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
	# Grab Metadata from all_media or navidromeDB.media_file
	if table == 0:
		cur = con.execute("SELECT song_id, album_id, artist_id, path, title, album, artist, track_number, created, genre FROM all_media WHERE song_id=?", (song_id,))
	elif table == 1:
		cur = con.execute("SELECT id, album_id, artist_id, path, title, album, artist, track_number, created_at, genre FROM navidromeDB.media_file WHERE id=?", (song_id,))
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

def printBoth(string, datePrint=True):
	if datePrint:
		now = datetime.now(tz=ZoneInfo("America/New_York"))
		string = now.strftime('[%a, %B %d, %Y @ %H:%M:%S %Z] :\t ') + str(string) + "\n"
	printToLog(string)
	print(string)