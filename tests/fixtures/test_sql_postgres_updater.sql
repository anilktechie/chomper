DROP TABLE IF EXISTS updater_test;

CREATE TABLE updater_test (
    id SERIAL,
    first_name varchar(420),
    last_name varchar(420),
    age integer,
    PRIMARY KEY (id)
);

INSERT INTO updater_test (first_name, last_name, age) VALUES ('Jeff', null, null);
