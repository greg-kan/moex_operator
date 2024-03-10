/*
 * shares_history
 */

--truncate table history.shares_history; 
create table history.shares_history (
 boardid varchar(32),
 tradedate date,
 shortname varchar(256),
 secid varchar(64),
 numtrades numeric(19,6),
 value numeric(19,6),
 open numeric(19,6),
 low numeric(19,6),
 high numeric(19,6),
 legalcloseprice numeric(19,6),
 waprice numeric(19,6),
 close numeric(19,6),
 volume numeric(19,6),
 marketprice2 numeric(19,6),
 marketprice3 numeric(19,6),
 admittedquote numeric(19,6),
 mp2valtrd numeric(19,6),
 marketprice3tradesvalue numeric(19,6),
 admittedvalue numeric(19,6),
 waval numeric(19,6),
 tradingsession int4,
 currencyid varchar(16),
 trendclspr numeric(19,6),
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);

create table history.shares_history_2023_2023
(like history.shares_history INCLUDING all);

create table history.shares_history_2020_2022
(like history.shares_history INCLUDING all);

create table history.shares_history_2010_2019
(like history.shares_history INCLUDING all);

CREATE OR REPLACE FUNCTION history.f_save_shares_history(json_data json)
RETURNS int
as $$
DECLARE 
  last_date date;
  curr_date date;
  row_cnt int := 0;
begin
 
  create temporary table temp_shares_history
  (like history.shares_history INCLUDING all);
 
  insert into temp_shares_history
  select * from json_to_recordset(json_data)
  as x("boardid" varchar(32), "tradedate" date, "shortname" varchar(256), "secid" varchar(64), "numtrades" numeric(19,6),
       "value" numeric(19,6), "open" numeric(19,6), "low" numeric(19,6), "high" numeric(19,6), "legalcloseprice" numeric(19,6),
       "waprice" numeric(19,6), "close" numeric(19,6), "volume" numeric(19,6), "marketprice2" numeric(19,6),
       "marketprice3" numeric(19,6), "admittedquote" numeric(19,6), "mp2valtrd" numeric(19,6), "marketprice3tradesvalue" numeric(19,6),
       "admittedvalue" numeric(19,6), "waval" numeric(19,6), "tradingsession" int4, "currencyid" varchar(16),
       "trendclspr" numeric(19,6), "sess_num" integer);
      
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

/*
 * bonds_history
 */

--truncate table history.bonds_history;
create table history.bonds_history (
 boardid varchar(32),
 tradedate date,
 shortname varchar(256),
 secid varchar(64),
 numtrades numeric(19,6),
 value numeric(19,6),
 low numeric(19,6),
 high numeric(19,6),
 close numeric(19,6),
 legalcloseprice numeric(19,6),
 ACCINT double precision,
 waprice numeric(19,6),
 YIELDCLOSE numeric(19,6),
 open numeric(19,6),
 volume numeric(19,6),
 marketprice2 numeric(19,6),
 marketprice3 numeric(19,6),
 admittedquote numeric(19,6),
 mp2valtrd numeric(19,6),
 marketprice3tradesvalue numeric(19,6),
 admittedvalue numeric(19,6),
 MATDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 DURATION double precision,
 YIELDATWAP numeric(19,6),
 IRICPICLOSE double precision,
 BEICLOSE double precision,
 COUPONPERCENT numeric(19,6),
 COUPONVALUE numeric(19,6),
 BUYBACKDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 LASTTRADEDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 facevalue double precision,
 CURRENCYID varchar(16),
 CBRCLOSE double precision,
 YIELDTOOFFER double precision,
 YIELDLASTCOUPON double precision,
 OFFERDATE varchar(16),
 FACEUNIT varchar(16),
 TRADINGSESSION integer,
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);


CREATE OR REPLACE FUNCTION history.f_save_bonds_history(json_data json)
RETURNS int
as $$
DECLARE 
  last_date date;
  curr_date date;
  row_cnt int := 0;
begin
 
  create temporary table temp_bonds_history
  (like history.bonds_history INCLUDING all);
 
  insert into temp_bonds_history
  select * from json_to_recordset(json_data)
  as x("boardid" varchar(32), "tradedate" date, "shortname" varchar(256), "secid" varchar(64), "numtrades" numeric(19,6),
       "value" numeric(19,6), "low" numeric(19,6), "high" numeric(19,6), "close" numeric(19,6), "legalcloseprice" numeric(19,6),
       "accint" double precision, "waprice" numeric(19,6), "yieldclose" numeric(19,6), "open" numeric(19,6),
       "volume" numeric(19,6), "marketprice2" numeric(19,6), "marketprice3" numeric(19,6), "admittedquote" numeric(19,6),
       "mp2valtrd" numeric(19,6), "marketprice3tradesvalue" numeric(19,6), "admittedvalue" numeric(19,6), "matdate" varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
       "duration" double precision, "yieldatwap" numeric(19,6), "iricpiclose" double precision, "beiclose" double precision,
       "couponpercent" numeric(19,6), "couponvalue" numeric(19,6), "buybackdate" varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
       "lasttradedate" varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
       "facevalue" double precision, "currencyid" varchar(16), "cbrclose" double precision, "yieldtooffer" double precision,
       "yieldlastcoupon" double precision, "offerdate" varchar(16), "faceunit" varchar(16), "tradingsession" integer, "sess_num" integer);
      
  select max(tradedate) into curr_date
    from temp_bonds_history;      
      
  select max(tradedate) into last_date
    from history.bonds_history;
    
  if (last_date is null) or ((last_date is not null) and (curr_date > last_date)) then
    insert into history.bonds_history
    select * from temp_bonds_history;
    
    GET DIAGNOSTICS row_cnt = ROW_COUNT;
  end if;
 
  return row_cnt;
 
exception
  when others then
    return -1; --error
end;

$$
LANGUAGE plpgsql;


