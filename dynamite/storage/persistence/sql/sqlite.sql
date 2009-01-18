CREATE TABLE IF NOT EXISTS key_values (
    id integer primary key,
    key varchar(255), 
    value blob(1024), 
    date timestamp
);
