-- Financial data pipeline schema. Safe to run repeatedly.

BEGIN;

CREATE TABLE IF NOT EXISTS sources (
    source_id  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT NOT NULL UNIQUE,
    base_url   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS series (
    series_id    INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_id    INTEGER NOT NULL REFERENCES sources (source_id),
    external_id  TEXT NOT NULL,
    name         TEXT NOT NULL,
    unit         TEXT NOT NULL,
    frequency    TEXT NOT NULL,
    UNIQUE (source_id, external_id)
);

CREATE TABLE IF NOT EXISTS observations (
    series_id         INTEGER NOT NULL REFERENCES series (series_id),
    observation_date  DATE NOT NULL,
    value             NUMERIC NOT NULL,
    fetched_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (series_id, observation_date)
);

COMMIT;
