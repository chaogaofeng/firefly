DROP TABLE IF EXISTS daitasks;
CREATE TABLE daitasks (
  seq            INTEGER         PRIMARY KEY AUTOINCREMENT,
  id             UUID            NOT NULL,
  namespace      VARCHAR(64)     NOT NULL,
  name           VARCHAR(64)     NOT NULL,
  desc           TEXT,
  requester      VARCHAR(1024),
  status         INTEGER,
  hosts          TEXT,
  datasets       TEXT,
  params         TEXT,
  results        TEXT,
  author         VARCHAR(1024)   NOT NULL,
  message_id     UUID,
  message_hash   CHAR(64),
  tx_type        VARCHAR(64),
  tx_id            UUID,
  blockchain_event UUID,
  created          BIGINT          NOT NULL
);


CREATE UNIQUE INDEX daitasks_id ON daitasks(id);
CREATE UNIQUE INDEX daitasks_name ON daitasks(namespace,name);