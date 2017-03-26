DROP TABLE IF EXISTS truncator_test;

CREATE TABLE truncator_test (
    id SERIAL,
    first_name varchar(420),
    PRIMARY KEY (id)
);

INSERT INTO truncator_test (first_name) VALUES ('Jeff');
INSERT INTO truncator_test (first_name) VALUES ('Annie');
INSERT INTO truncator_test (first_name) VALUES ('Britta');
