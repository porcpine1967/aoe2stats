CREATE TABLE public.matches (
    id bigint NOT NULL,
    match_id text,
    player_id integer,
    rating integer,
    civ_id smallint,
    map_type smallint,
    rating_type smallint,
    started integer,
    version text,
    won boolean,
    mirror boolean,
    team_size smallint,
    game_type smallint,
    finished integer
);

CREATE TABLE public.results (
    id bigint NOT NULL,
    week text,
    civ_id text,
    team_size text,
    map_category text,
    methodology text,
    metric text,
    compound boolean,
    rank smallint,
    pct numeric(6,5)
);

CREATE TABLE public.week_counts (
    id bigint NOT NULL,
    week text,
    match_count integer
);

CREATE TABLE public.tournaments (
    id serial primary key,
    name text,
    url text,
    game text,
    tier text,
    start_date date,
    end_date date,
    prize text,
    participant_count integer,
    first_place text,
    first_place_url text,
    second_place text,
    description text,
    series text,
    organizers text,
    sponsors text,
    game_mode text,
    format text,
    team boolean,
    runners_up text
);

CREATE TABLE public.player_results (
    id serial primary key,
	player_url text,
	player_place text,
	player_prize text,
	tournament_url text
);

CREATE UNIQUE INDEX player_results_player_tournament
ON player_results(player_url, tournament_url);

CREATE TABLE public.scores (
id serial primary key,
evaluation_date date,
player_url text,
score int,
scorer text
);
CREATE UNIQUE INDEX player_score_index
ON scores(player_url, scorer, evaluation_date);
