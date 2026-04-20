alter table users
add column expire_time integer;

alter table users
add column user_groups text default '[]';

alter table users
add column is_pro integer not null default 0;

alter table users
add column graphs_count integer not null default 0;

alter table users
add column storage_count integer not null default 0;

alter table users
add column updated_at integer;