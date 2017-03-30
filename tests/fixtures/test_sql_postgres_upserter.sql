DROP TABLE IF EXISTS upserter_test;

CREATE TABLE upserter_test (
    id SERIAL,
    first_name varchar(420),
    last_name varchar(420),
    age integer,
    created_at TIMESTAMP WITH TIME ZONE,
    updated_at TIMESTAMP WITH TIME ZONE,
    PRIMARY KEY (id)
);

TRUNCATE TABLE upserter_test RESTART IDENTITY;

INSERT INTO upserter_test (first_name, last_name, age, created_at, updated_at) VALUES ('Jeff', null, null, '2017-01-01 00:00:00.000000+0', '2017-01-01 00:00:00.000000+0');
INSERT INTO upserter_test (first_name, last_name, age, created_at, updated_at) VALUES ('Britta', 'Perry', null, '2017-01-02 00:00:00.000000+0', '2017-01-02 00:00:00.000000+0');
