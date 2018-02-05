
create table rooms (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	room_name TEXT,
	floor TEXT
);

create table scheduling (
	day_of_week INTEGER,
	seconds_of_day INTEGER,
	room_id INTEGER,
	temperature REAL,
	FOREIGN KEY(room_id) REFERENCES rooms(id)
);

create table overrides (
	room_id INTEGER,
	temperature REAL,
	start_time INTEGER,
	end_time INTEGER,
	FOREIGN KEY(room_id) REFERENCES rooms(id)
);

INSERT INTO rooms ("room_name","floor") VALUES ("livingroom","groundfloor");

