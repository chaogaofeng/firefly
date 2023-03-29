CREATE TABLE trainingmodels (
  seq         INTEGER         PRIMARY KEY AUTOINCREMENT,
  id          UUID            NOT NULL,
  namespace   VARCHAR(64)     NOT NULL,
  message_id  UUID            NOT NULL,  
  flowId      VARCHAR(128)    NOT NULL,
  flowName    VARCHAR(128)    NOT NULL,
  flowParties TEXT            NOT NULL,
  flowItems   TEXT            NOT NULL
);

CREATE UNIQUE INDEX trainingmodels_id ON trainingmodels(id);
CREATE UNIQUE INDEX trainingmodels_unique ON trainingmodels(namespace,flowName);
