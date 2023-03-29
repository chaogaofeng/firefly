CREATE TABLE trainingjobs (
  seq         SERIAL          PRIMARY KEY,
  id          UUID            NOT NULL,
  namespace   VARCHAR(64)     NOT NULL,
  message_id  UUID            NOT NULL,
  name        VARCHAR(64)     NOT NULL,
  status      VARCHAR(64)     NOT NULL,
  task        UUID            NOT NULL,
  pipeline    TEXT            NOT NULL,
  created     BIGINT          NOT NULL,
  updated     BIGINT
);

CREATE UNIQUE INDEX trainingjobs_id ON trainingjobs(id);