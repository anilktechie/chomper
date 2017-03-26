DROP TABLE IF EXISTS feeder_test;

CREATE TABLE feeder_test (
    id SERIAL,
    first_name varchar(420),
    last_name varchar(420),
    age integer,
    PRIMARY KEY (id)
);

INSERT INTO feeder_test (first_name, last_name, age) VALUES ('Jeff', 'Winger', 32);
INSERT INTO feeder_test (first_name, last_name, age) VALUES ('Annie', 'Edison', 23);
INSERT INTO feeder_test (first_name, last_name, age) VALUES ('Britta', 'Perry', 28);
INSERT INTO feeder_test (first_name, last_name, age) VALUES ('Abed', 'Nadir', 26);
INSERT INTO feeder_test (first_name, last_name, age) VALUES ('Troy', 'Barnes', 24);
