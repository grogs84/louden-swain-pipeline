CREATE TABLE person (
  person_id UUID PRIMARY KEY,
  first_name TEXT,
  last_name TEXT
);

CREATE TABLE role (
  role_id UUID PRIMARY KEY,
  person_id UUID REFERENCES person(person_id),
  role_type TEXT -- 'wrestler', 'coach', etc.
);

CREATE TABLE school (
    school_id UUID PRIMARY KEY,
    name TEXT,
    location TEXT
);

CREATE TABLE tournament (
  tournament_id UUID PRIMARY KEY,
  name TEXT,
  year INT,
  location TEXT
);

CREATE TABLE participant (
    participant_id UUID PRIMARY KEY,
    role_id UUID REFERENCES role(role_id),
    school_id UUID REFERENCES school(school_id),
    year INT,
    weight_class TEXT,
    seed INT
);

CREATE TABLE match (
  match_id UUID PRIMARY KEY,
  round TEXT,
  round_order INT,
  bracket_order INT,
  tournament_id UUID REFERENCES tournament(tournament_id)
);

CREATE TABLE participant_match (
  match_id UUID REFERENCES match(match_id),
  participant_id UUID REFERENCES participant(participant_id),
  is_winner BOOLEAN,
  score INT,
  result_type TEXT, -- 'FALL', 'DEC', 'TECH', etc.
  fall_time INTERVAL,
  next_match_id UUID REFERENCES match(match_id),
  PRIMARY KEY (match_id, participant_id)
);


-- Sample query to retrieve match details with participants and their results
-- SELECT jsonb_build_object(
--   'id', m.match_id,
--   'name', m.round,
--   'nextMatchId', pm_winner.next_match_id,
--   'startTime', m.start_time,
--   'state', 'DONE',
--   'participants', (
--     SELECT jsonb_agg(jsonb_build_object(
--       'id', p.participant_id,
--       'name', p.name,
--       'isWinner', pm.is_winner,
--       'resultText', 
--         CASE 
--           WHEN pm.is_winner THEN 
--             'WON ' || pm.score || COALESCE(' (' || pm.result_type || ')', '')
--           ELSE 
--             'LOST ' || pm.score || COALESCE(' (' || pm.result_type || ')', '')
--         END,
--       'status', 'PLAYED'
--     ))
--     FROM participant_match pm
--     JOIN participant p ON pm.participant_id = p.participant_id
--     WHERE pm.match_id = m.match_id
--   )
-- )
-- FROM match m
-- LEFT JOIN participant_match pm_winner
--   ON m.match_id = pm_winner.match_id AND pm_winner.is_winner = true
-- ORDER BY m.bracket_order;

