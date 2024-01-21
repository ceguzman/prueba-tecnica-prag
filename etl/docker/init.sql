-- init.sql
CREATE TABLE IF NOT EXISTS db_batch.public.my_user
(
    user_id   integer primary key,
    price     integer,
    timestamp timestamp
);
