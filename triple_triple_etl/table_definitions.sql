CREATE TABLE games (
  id VARCHAR(20),
  start_time TIMESTAMP,
  home_team_id INTEGER,
  visitor_team_id INTEGER,
  PRIMARY KEY (id)
);

CREATE TABLE players (
  id INTEGER,
  last_name VARCHAR(20),
  first_name VARCHAR(20),
  team_id INTEGER,
  jersey_number INTEGER,
  position VARCHAR(5),
  start_date DATE,
  end_date DATE,
  PRIMARY KEY (id, start_date)
);

CREATE TABLE teams (
  id INTEGER,
  name VARCHAR,
  conference VARCHAR(4),
  division VARCHAR(15),
  city VARCHAR(20),
  state CHAR(2),
  zipcode INTEGER,
  start_date DATE,
  end_date DATE,
  PRIMARY KEY (id, start_date)
);

CREATE TABLE game_positions (
  game_id VARCHAR(20),
  event_id INTEGER,
  time_stamp TIMESTAMP,
  period INTEGER,
  period_clock FLOAT,
  shot_clock FLOAT,
  team_id INTEGER,
  player_id INTEGER,
  x_coordinate FLOAT,
  y_coordinate FLOAT,
  z_coordinate FLOAT,
  PRIMARY KEY (time_stamp),
  FOREIGN KEY (game_id) REFERENCES games (id),
);
  
