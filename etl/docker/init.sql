-- init.sql
CREATE TABLE IF NOT EXISTS db_batch.public.my_user
(
    id SERIAL PRIMARY KEY,
    user_id   integer ,
    price     integer,
    timestamp date
);
