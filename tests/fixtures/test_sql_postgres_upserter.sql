DROP TABLE IF EXISTS upserter_test;

CREATE TABLE upserter_test (
    id SERIAL,
    first_name varchar(420),
    last_name varchar(420),
    age integer,
    PRIMARY KEY (id)
);

INSERT INTO upserter_test (first_name, last_name, age) VALUES ('Jeff', null, null);
