DROP TABLE IF EXISTS daiexecutors;
CREATE TABLE daiexecutors (
  seq            INTEGER         PRIMARY KEY AUTOINCREMENT,
  id             UUID            NOT NULL,
  namespace      VARCHAR(64)     NOT NULL,
  name           VARCHAR(64)     NOT NULL,
  tag            VARCHAR(1024),
  type           INTEGER,
  address        VARCHAR(1024),
  url            VARCHAR(1024),
  role           INTEGER,
  status         INTEGER,
  author         VARCHAR(1024)   NOT NULL,
  message_id     UUID,
  message_hash   CHAR(64),
  tx_type        VARCHAR(64),
  tx_id            UUID,
  blockchain_event UUID,
  created          BIGINT          NOT NULL
);


CREATE UNIQUE INDEX daiexecutors_id ON daiexecutors(id);
CREATE UNIQUE INDEX daiexecutors_name ON daiexecutors(namespace,name);