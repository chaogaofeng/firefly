CREATE TABLE trainingnodes (
  seq         INTEGER         PRIMARY KEY AUTOINCREMENT,
  id          UUID            NOT NULL,
  namespace   VARCHAR(64)     NOT NULL,
  message_id  UUID            NOT NULL,
  name        VARCHAR(128)    NOT NULL,
  parent      VARCHAR(128)    NOT NULL,
  head        VARCHAR(64)     NOT NULL,
  address     VARCHAR(64)     NOT NULL,
  port        INTEGER         NOT NULL,
  range_start INTEGER         NOT NULL,
  range_end   INTEGER         NOT NULL,
  status        INTEGER       NOT NULL,
  description VARCHAR(4096)
);

CREATE UNIQUE INDEX trainingnodes_id ON trainingnodes(id);
CREATE UNIQUE INDEX trainingnodes_unique ON trainingnodes(namespace,name);