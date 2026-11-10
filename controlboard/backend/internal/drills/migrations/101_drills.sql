-- Incident Drills and their leaderboard (schema controlboard, engine migrations 1xx).
CREATE TABLE drills (
    id           text PRIMARY KEY,
    no           int NOT NULL UNIQUE,
    status       text NOT NULL,
    player       text NOT NULL,
    scenario     text NOT NULL,
    score        double precision,
    duration_sec double precision,
    started_at   timestamptz NOT NULL,
    doc          jsonb NOT NULL
);
CREATE INDEX drills_leaderboard ON drills (score DESC) WHERE status = 'solved';
