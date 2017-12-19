CREATE TABLE groups (
  id                        SERIAL                   NOT NULL PRIMARY KEY,
  name                      TEXT                     NOT NULL
);
CREATE TABLE employees (
  id                        SERIAL                   NOT NULL PRIMARY KEY,
  first_name                TEXT                     NOT NULL,
  last_name                 TEXT                     NOT NULL,
  manager_id                INTEGER                  NULL REFERENCES employees (id),
  group_id                  INTEGER                  NULL REFERENCES groups (id)
);
CREATE TABLE tickets (
  id                        SERIAL                   NOT NULL PRIMARY KEY,
  author_id                 INTEGER                  NOT NULL REFERENCES employees (id),
  subject                   TEXT                     NOT NULL,
  message                   TEXT                     NOT NULL
);