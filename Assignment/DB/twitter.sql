

drop database if exists Twitter;
create database Twitter;


use Twitter; 
create table Tweet(
	tweet_id INT NOT NULL,
    user_id INT NOT NULL,
    tweet_ts DATETIME,
    tweet_text VARCHAR(140)
);



create table Follows(
	user_id INT NOT NULL,
    follows_id INT NOT NULL
);



