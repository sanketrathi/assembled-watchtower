-- Watchtower durable foundation. Keep this migration append-only after release.
CREATE SEQUENCE semantic_commit_seq;
CREATE TABLE occurrences (
    occurrence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    ingest_position BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    source TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    stream_position BIGINT,
    idempotency_key TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    payload JSONB NOT NULL,
    payload_hash TEXT NOT NULL,
    UNIQUE (source, idempotency_key),
    UNIQUE (source, stream_position)
);
CREATE UNIQUE INDEX occurrences_api_key_idx ON occurrences (idempotency_key)
    WHERE source = 'api';

CREATE TABLE occurrence_processing (
    occurrence_id BIGINT PRIMARY KEY REFERENCES occurrences ON DELETE CASCADE,
    status TEXT NOT NULL CHECK (status IN ('pending','processing','processed','failed')),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE queue_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurrence_id BIGINT NOT NULL REFERENCES occurrences,
    queue_id TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    stream_position BIGINT NOT NULL,
    tickets_waiting BIGINT NOT NULL CHECK (tickets_waiting >= 0),
    longest_wait_sec BIGINT NOT NULL CHECK (longest_wait_sec >= 0),
    sla_target_sec BIGINT NOT NULL CHECK (sla_target_sec >= 0),
    agents_available BIGINT NOT NULL CHECK (agents_available >= 0),
    agents_on_call BIGINT NOT NULL CHECK (agents_on_call >= 0),
    volume_last_15m BIGINT NOT NULL CHECK (volume_last_15m >= 0),
    volume_forecast_next_15m BIGINT CHECK (volume_forecast_next_15m IS NULL OR volume_forecast_next_15m >= 0),
    UNIQUE (occurrence_id), UNIQUE (queue_id, effective_at, stream_position, observation_id)
);
CREATE INDEX queue_observations_current_idx ON queue_observations(queue_id, effective_at DESC);

CREATE TABLE agent_state_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurrence_id BIGINT NOT NULL REFERENCES occurrences,
    agent_id TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    stream_position BIGINT NOT NULL,
    previous_state TEXT,
    new_state TEXT NOT NULL,
    previous_state_duration_sec BIGINT CHECK (previous_state_duration_sec IS NULL OR previous_state_duration_sec >= 0),
    queue_ids JSONB,
    UNIQUE (occurrence_id)
);
CREATE INDEX agent_state_observations_current_idx ON agent_state_observations(agent_id, effective_at DESC);

CREATE TABLE adherence_observations (
    observation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurrence_id BIGINT NOT NULL REFERENCES occurrences,
    agent_id TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    stream_position BIGINT NOT NULL,
    scheduled_state TEXT NOT NULL,
    actual_state TEXT NOT NULL,
    in_violation BOOLEAN NOT NULL,
    violation_started_at TIMESTAMPTZ,
    queue_ids JSONB NOT NULL,
    UNIQUE (occurrence_id)
);
CREATE INDEX adherence_observations_current_idx ON adherence_observations(agent_id, effective_at DESC);

CREATE TABLE queue_state_current (
    queue_id TEXT PRIMARY KEY,
    observation_id BIGINT NOT NULL REFERENCES queue_observations,
    effective_at TIMESTAMPTZ NOT NULL,
    stream_position BIGINT NOT NULL
);
CREATE TABLE agent_state_current (
    agent_id TEXT PRIMARY KEY,
    observation_id BIGINT NOT NULL REFERENCES agent_state_observations,
    effective_at TIMESTAMPTZ NOT NULL,
    stream_position BIGINT NOT NULL
);
CREATE TABLE adherence_current (
    agent_id TEXT PRIMARY KEY,
    observation_id BIGINT NOT NULL REFERENCES adherence_observations,
    effective_at TIMESTAMPTZ NOT NULL,
    stream_position BIGINT NOT NULL
);

CREATE TABLE rule_resources (
    rule_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('draft','active','paused','archived')),
    active_revision BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE rule_revisions (
    rule_id TEXT NOT NULL REFERENCES rule_resources ON DELETE CASCADE,
    revision BIGINT NOT NULL,
    definition JSONB NOT NULL,
    compiled_target JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (rule_id, revision)
);
ALTER TABLE rule_resources ADD CONSTRAINT active_revision_fk
    FOREIGN KEY (rule_id, active_revision) REFERENCES rule_revisions(rule_id, revision);

CREATE TABLE groups (
    group_id TEXT PRIMARY KEY,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('queue','agent')),
    name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE group_revisions (
    group_id TEXT NOT NULL REFERENCES groups ON DELETE CASCADE,
    revision BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (group_id, revision)
);
CREATE TABLE group_revision_members (
    group_id TEXT NOT NULL, revision BIGINT NOT NULL, subject_id TEXT NOT NULL,
    PRIMARY KEY (group_id, revision, subject_id),
    FOREIGN KEY (group_id, revision) REFERENCES group_revisions(group_id, revision) ON DELETE CASCADE
);
CREATE TABLE rule_revision_targets (
    rule_id TEXT NOT NULL, revision BIGINT NOT NULL, subject_kind TEXT NOT NULL, subject_id TEXT NOT NULL,
    PRIMARY KEY (rule_id, revision, subject_kind, subject_id),
    FOREIGN KEY (rule_id, revision) REFERENCES rule_revisions(rule_id, revision) ON DELETE CASCADE
);

CREATE TABLE condition_trackers (
    condition_key TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('queue','agent')),
    subject_id TEXT NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('idle','triggering','active','clearing')),
    true_since TIMESTAMPTZ,
    timer_generation BIGINT NOT NULL DEFAULT 0,
    evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE timers (
    timer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    condition_key TEXT NOT NULL REFERENCES condition_trackers ON DELETE CASCADE,
    due_at TIMESTAMPTZ NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('trigger','clear')),
    generation BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending','claimed','done','cancelled')),
    claim_token TEXT,
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (condition_key, generation)
);
CREATE INDEX timers_due_idx ON timers(status, due_at);

CREATE TABLE condition_episodes (
    episode_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    condition_key TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    generation BIGINT NOT NULL,
    subject_kind TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    opened_at TIMESTAMPTZ NOT NULL,
    closed_at TIMESTAMPTZ,
    trigger_evidence JSONB NOT NULL,
    clear_evidence JSONB,
    UNIQUE (condition_key, generation)
);
CREATE UNIQUE INDEX condition_episodes_open_idx ON condition_episodes(condition_key) WHERE closed_at IS NULL;

CREATE TABLE semantic_transitions (
    transition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    commit_seq BIGINT NOT NULL DEFAULT nextval('semantic_commit_seq'),
    idempotency_key TEXT NOT NULL UNIQUE,
    condition_key TEXT NOT NULL,
    episode_id BIGINT REFERENCES condition_episodes,
    transition_type TEXT NOT NULL CHECK (transition_type IN ('trigger','clear')),
    occurred_at TIMESTAMPTZ NOT NULL,
    evidence JSONB NOT NULL,
    occurrence_id BIGINT REFERENCES occurrences,
    timer_id BIGINT REFERENCES timers,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE semantic_transitions ADD CONSTRAINT semantic_transition_provenance_ck CHECK ((occurrence_id IS NOT NULL) <> (timer_id IS NOT NULL));

CREATE TABLE alert_series (
    alert_series_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    rule_id TEXT NOT NULL,
    subject_kind TEXT NOT NULL CHECK (subject_kind IN ('queue','agent')),
    subject_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (rule_id, subject_kind, subject_id)
);
CREATE TABLE alert_generations (
    alert_generation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    alert_series_id BIGINT NOT NULL REFERENCES alert_series ON DELETE CASCADE,
    generation BIGINT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open','recovered')),
    opened_at TIMESTAMPTZ NOT NULL,
    recovered_at TIMESTAMPTZ,
    UNIQUE (alert_series_id, generation)
);
CREATE TABLE alert_transitions (
    alert_transition_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    commit_seq BIGINT NOT NULL DEFAULT nextval('semantic_commit_seq'),
    alert_generation_id BIGINT NOT NULL REFERENCES alert_generations ON DELETE CASCADE,
    semantic_transition_id BIGINT REFERENCES semantic_transitions,
    transition_type TEXT NOT NULL CHECK (transition_type IN ('open','recovery')),
    occurred_at TIMESTAMPTZ NOT NULL,
    UNIQUE (alert_generation_id, transition_type)
);
CREATE TABLE alert_contributors (
    alert_generation_id BIGINT NOT NULL REFERENCES alert_generations ON DELETE CASCADE,
    episode_id BIGINT NOT NULL REFERENCES condition_episodes,
    contributed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    cleared_at TIMESTAMPTZ,
    PRIMARY KEY (alert_generation_id, episode_id),
    UNIQUE (episode_id)
);

CREATE TABLE notification_intents (
    intent_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    commit_seq BIGINT NOT NULL DEFAULT nextval('semantic_commit_seq'),
    dedupe_key TEXT NOT NULL UNIQUE,
    alert_generation_id BIGINT NOT NULL REFERENCES alert_generations,
    alert_transition_id BIGINT REFERENCES alert_transitions,
    transition_type TEXT NOT NULL CHECK (transition_type IN ('open','recovery')),
    audience TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending','claimed','delivered','failed')),
    payload JSONB NOT NULL,
    claim_token TEXT,
    claimed_until TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    delivered_at TIMESTAMPTZ
);
CREATE INDEX notification_intents_claim_idx ON notification_intents(state, created_at);
CREATE UNIQUE INDEX notification_intents_transition_audience_idx
    ON notification_intents(alert_transition_id, audience, transition_type)
    WHERE alert_transition_id IS NOT NULL;
CREATE TABLE delivery_attempts (
    attempt_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES notification_intents ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    state TEXT NOT NULL CHECK (state IN ('started','succeeded','failed')),
    visible_message TEXT,
    error TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    finished_at TIMESTAMPTZ,
    UNIQUE (intent_id, attempt_number)
);

CREATE TABLE notification_deliveries (
    intent_id BIGINT PRIMARY KEY REFERENCES notification_intents ON DELETE CASCADE,
    attempt_id BIGINT NOT NULL REFERENCES delivery_attempts,
    delivered_at TIMESTAMPTZ NOT NULL,
    outcome TEXT NOT NULL,
    visible_message TEXT NOT NULL
);

CREATE TABLE runtime_clock (
    clock_id BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (clock_id),
    logical_now TIMESTAMPTZ,
    greatest_observed_at TIMESTAMPTZ
);
INSERT INTO runtime_clock(clock_id) VALUES(TRUE);

CREATE TABLE IF NOT EXISTS schema_migrations (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    checksum TEXT NOT NULL DEFAULT '',
    applied_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);
