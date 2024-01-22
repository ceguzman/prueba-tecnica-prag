CREATE TABLE IF NOT EXISTS db_batch.public.my_user
(
    id        SERIAL PRIMARY KEY,
    user_id   integer,
    price     integer,
    timestamp date
);

CREATE TABLE IF NOT EXISTS db_batch.public.my_user_validation
(
    id        SERIAL PRIMARY KEY,
    user_id   integer,
    price     integer,
    timestamp date
);
