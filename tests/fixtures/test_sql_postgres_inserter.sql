DROP TABLE IF EXISTS inserter_test;

CREATE TABLE inserter_test (
    id SERIAL,
    first_name varchar(420),
    last_name varchar(420),
    age integer,
    PRIMARY KEY (id)
);
