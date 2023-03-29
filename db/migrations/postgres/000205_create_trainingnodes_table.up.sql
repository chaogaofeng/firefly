BEGIN;
CREATE TABLE trainingnodes (
  seq         SERIAL          PRIMARY KEY,
  id          UUID            NOT NULL,
  namespace   VARCHAR(64)     NOT NULL,
  message_id  UUID            NOT NULL,
  name        VARCHAR(128)     NOT NULL,
  parent      VARCHAR(128)     NOT NULL,
  head        VARCHAR(64)     NOT NULL,
  address     VARCHAR(64)     NOT NULL,
  port        INT     NOT NULL,
  range_start INT     NOT NULL,
  range_end   INT     NOT NULL,
  status      INT     NOT NULL,
  description VARCHAR(4096)
);

CREATE UNIQUE INDEX trainingnodes_id ON trainingnodes(id);
CREATE UNIQUE INDEX trainingnodes_unique ON trainingnodes(namespace,name);
COMMIT;