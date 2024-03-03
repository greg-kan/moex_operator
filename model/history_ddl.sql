
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
RETURNS int
as $$
DECLARE 
  last_date date;
  curr_date date;
  row_cnt int := 0;
begin
 
  create temporary table temp_bonds_base
  (like reference.bonds_base INCLUDING all);
 
 
  insert into reference.bonds_base
  select * from json_to_recordset(json_data)
  as x("id" integer, "secid" varchar(64), "shortname" varchar(256), "regnumber" varchar(256), "name" varchar(1024),
       "isin" varchar(64), "is_traded" integer, "emitent_id" integer, "emitent_title" varchar(1024), "emitent_inn" varchar(32),
       "emitent_okpo" varchar(32), "gosreg" varchar(256), "type" varchar(128), "group" varchar(128), "primary_boardid" varchar(32),
       "marketprice_boardid" varchar(32));  
 
  insert into temp_shares_history
  select * from json_to_recordset(json_data)
  as x("boardid" varchar(32), "tradedate" date, "shortname" varchar(256), "secid" varchar(64), "numtrades" numeric(19,6),
       "value" numeric(19,6), "open" numeric(19,6), "low" numeric(19,6), "high" numeric(19,6), "legalcloseprice" numeric(19,6),
       "waprice" numeric(19,6), "close" numeric(19,6), "volume" numeric(19,6), "marketprice2" numeric(19,6),
       "marketprice3" numeric(19,6), "admittedquote" numeric(19,6), "mp2valtrd" numeric(19,6), "marketprice3tradesvalue" numeric(19,6),
       "admittedvalue" numeric(19,6), "waval" numeric(19,6), "tradingsession" int4, "currencyid" varchar(16),
       "trendclspr" numeric(19,6));
      
  select max(tradedate) into curr_date
    from temp_shares_history;      
      
  select max(tradedate) into last_date
    from history.shares_history;
    
  if (last_date is null) or ((last_date is not null) and (curr_date > last_date)) then
    insert into history.shares_history
    select * from temp_shares_history;
    
    GET DIAGNOSTICS row_cnt = ROW_COUNT;
  end if;
 
  return row_cnt;
 
exception
  when others then
    return -1; --error
end;

$$
LANGUAGE plpgsql;


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
https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.json
https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQCB/securities.json
select distinct primary_boardid from reference.bonds_base;
select distinct marketprice_boardid from reference.bonds_base;

primary_boardid
marketprice_boardid

select * from history.shares_list_on_date
  where secid  = 'SBER';

