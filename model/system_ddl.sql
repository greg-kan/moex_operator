CREATE SCHEMA sys;
--drop table sys.session;
--truncate table sys.session;
create table sys.session (
 session_number serial,
 session_time timestamp default current_timestamp
);


CREATE OR REPLACE FUNCTION sys.f_set_session()
 returns record
 LANGUAGE plpgsql
AS $function$
DECLARE 
  result record;
begin
  insert into sys.session (session_time) values (current_timestamp);
 
  select s.session_number, s.session_time into result
    from sys.session s
   where s.session_number = (select max(session_number) from sys.session);
 
  return result;
exception
  when others then
    return null; --error
end;
$function$;


--CREATE OR REPLACE FUNCTION sys.f_set_session()
-- RETURNS integer
-- LANGUAGE plpgsql
--AS $function$
--DECLARE 
--  result integer := 0;
--begin
--  insert into sys.session (session_time) values (current_timestamp);
--  select max(session_number) into result from sys.session;
--  return result;
--exception
--  when others then
--    return -1; --error
--end;
--$function$
--;
