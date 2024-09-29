select * from users --select query with all columns selections
select id from users --simple query with single select
select id, name from users --query with multiple select columns
select u.id, u.name from users --query with column specific table selection
select first_name as name from users --select query with column alias
select first_name as name from users where id = 1 --select query with where clause integer comparison
select first_name as name from users where first_name = 'stefan' --select query with where clause string comparison