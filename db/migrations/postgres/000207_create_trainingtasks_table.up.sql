BEGIN;
CREATE TABLE trainingtasks (
  seq         SERIAL          PRIMARY KEY,
  id          UUID            NOT NULL,
  namespace   VARCHAR(64)     NOT NULL,
  message_id  UUID            NOT NULL,
  name        VARCHAR(64)     NOT NULL,
  parties     VARCHAR(1024)   NOT NULL,
  model       UUID            NOT NULL
);

CREATE UNIQUE INDEX trainingtasks_id ON trainingtasks(id);
CREATE UNIQUE INDEX trainingtasks_unique ON trainingtasks(namespace,name);
COMMIT;