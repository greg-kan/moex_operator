
--drop table reference.bonds_base;
--truncate table reference.bonds_base;
create table reference.bonds_base (
 id integer,
 secid varchar(64),
 shortname varchar(256),
 regnumber  varchar(256),
 name varchar(1024),
 isin varchar(64),
 is_traded integer,
 emitent_id integer,
 emitent_title varchar(1024),
 emitent_inn varchar(32),
 emitent_okpo varchar(32),
 gosreg varchar(256),
 type varchar(128),
 "group" varchar(128),
 primary_boardid varchar(32),
 marketprice_boardid varchar(32),
 inserttimestamp timestamp DEFAULT current_timestamp,
 updatetimestamp timestamp
);


CREATE OR REPLACE FUNCTION reference.f_save_bonds_base(json_data json)
RETURNS text
as $$
DECLARE 
  curr_timestamp timestamp := current_timestamp;
  row_cnt_new int := 0;
  row_cnt_modified int := 0;
begin
 
  create temporary table temp_bonds_base
  (like reference.bonds_base INCLUDING all);
 
  create temporary table temp_bonds_base_modified
  (like reference.bonds_base INCLUDING all); 
 
 
  insert into temp_bonds_base
  select * from json_to_recordset(json_data)
  as x("id" integer, "secid" varchar(64), "shortname" varchar(256), "regnumber" varchar(256), "name" varchar(1024),
       "isin" varchar(64), "is_traded" integer, "emitent_id" integer, "emitent_title" varchar(1024), "emitent_inn" varchar(32),
       "emitent_okpo" varchar(32), "gosreg" varchar(256), "type" varchar(128), "group" varchar(128), "primary_boardid" varchar(32),
       "marketprice_boardid" varchar(32));  
 
  insert into temp_bonds_base_modified (   
  select "id", "secid", "shortname", "regnumber", "name", "isin", "is_traded", "emitent_id", "emitent_title",
         "emitent_inn", "emitent_okpo", "gosreg", "type", "group", "primary_boardid", "marketprice_boardid"
    from temp_bonds_base tbb
   where tbb."secid" in (select "secid" from reference.bonds_base)
 
  except

  select "id", "secid", "shortname", "regnumber", "name", "isin", "is_traded", "emitent_id", "emitent_title",
         "emitent_inn", "emitent_okpo", "gosreg", "type", "group", "primary_boardid", "marketprice_boardid"
    from reference.bonds_base bb
   where bb."secid" in (select "secid" from temp_bonds_base)
     and bb.updatetimestamp is null
  );
  
---------part two
  update reference.bonds_base
     set updatetimestamp = current_timestamp
   where "secid" in (select "secid" from temp_bonds_base_modified);
  
  insert into reference.bonds_base
  select * from temp_bonds_base_modified;
     
  GET DIAGNOSTICS row_cnt_modified = ROW_COUNT;
 
---------part three
  insert into reference.bonds_base
  select "id", "secid", "shortname", "regnumber", "name", "isin", "is_traded", "emitent_id", "emitent_title",
         "emitent_inn", "emitent_okpo", "gosreg", "type", "group", "primary_boardid", "marketprice_boardid"
    from temp_bonds_base
   where "secid" not in (select "secid" from reference.bonds_base);  
    
  GET DIAGNOSTICS row_cnt_new = ROW_COUNT;
 
  return 'new records: ' || row_cnt_new::text || ', modified records: ' || row_cnt_modified::text;
 
exception
  when others then
    return '-1'::text; --error
end;

$$
LANGUAGE plpgsql;


create table reference.bonds_base1
(like reference.bonds_base INCLUDING all);

insert into reference.bonds_base1
select * from reference.bonds_base;



select * from reference.bonds_base1; --7766

delete from reference.bonds_base1  --3426
 where emitent_id between 1000 and 2000;

update reference.bonds_base1 --14
set shortname = 'TEMPNAME'
where emitent_id = 2003;


select * from reference.bonds_base1
 where emitent_id = 2003
 order by secid, updatetimestamp;

--truncate table history.stock_shares_securities_history;
--truncate table history.shares_list_on_date;
--truncate table reference.bonds_initial;
--truncate table reference.bonds_list_on_date;

select *
  from ( 
select *,
       row_number() over(partition by secid order by secid, inserttimestamp desc) rn,
       count(*) over(partition by secid) cnt
  from reference.bonds_initial) a
 where a.cnt > 1;

select * from reference.bonds_base;

truncate table reference.shares_base;


