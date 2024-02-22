create schema history;
CREATE SCHEMA reference;

create table history.stock_shares_securities_history (
 boardid varchar(32),
 tradedate date,
 shortname varchar(256),----------------64
 secid varchar(64),
 numtrades numeric(15,2),
 value numeric(15,2),
 open numeric(15,2),
 low numeric(15,2),
 high numeric(15,2),
 legalcloseprice numeric(15,2),
 waprice numeric(15,2),
 close numeric(15,2),
 volume numeric(15,2),
 marketprice2 numeric(15,2),
 marketprice3 numeric(15,2),
 admittedquote numeric(15,2),
 mp2valtrd numeric(15,2),
 marketprice3tradesvalue numeric(15,2),
 admittedvalue numeric(15,2),
 waval numeric(15,2),
 tradingsession int4,
 currencyid varchar(16),
 trendclspr numeric(15,2),
 inserttimestamp timestamp DEFAULT current_timestamp
);


--truncate table history.stock_shares_securities_history_2023;
create table history.stock_shares_securities_history_2023_2023 (
 boardid varchar(32),
 tradedate date,
 shortname varchar(256),------------64
 secid varchar(64),
 numtrades numeric(15,2),
 value numeric(15,2),
 open numeric(15,2),
 low numeric(15,2),
 high numeric(15,2),
 legalcloseprice numeric(15,2),
 waprice numeric(15,2),
 close numeric(15,2),
 volume numeric(15,2),
 marketprice2 numeric(15,2),
 marketprice3 numeric(15,2),
 admittedquote numeric(15,2),
 mp2valtrd numeric(15,2),
 marketprice3tradesvalue numeric(15,2),
 admittedvalue numeric(15,2),
 waval numeric(15,2),
 tradingsession int4,
 currencyid varchar(16),
 trendclspr numeric(15,2)
);

----------------------------
--drop table history.shares_list_on_date;
--truncate table history.shares_list_on_date;
create table history.shares_list_on_date (
 secid varchar(64),
 boardid varchar(32),
 shortname varchar(256), ---------64
 prevprice numeric(15,2),
 lotsize integer,
 facevalue double precision,
 status varchar(10),
 boardname varchar(512),
 decimals integer,
 secname varchar(128),
 remarks varchar(32),
 marketcode varchar(32),
 instrid varchar(32),
 sectorid varchar(32),
 minstep double precision,
 prevwaprice numeric(15,2),
 faceunit varchar(32),
 prevdate varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 issuesize bigint,
 isin varchar(48),
 latname varchar(128),
 regnumber varchar(128),
 prevlegalcloseprice numeric(15,2),
 currencyid varchar(32),
 sectype varchar(10),
 listlevel integer,
 settledate date,
 inserttimestamp timestamp DEFAULT current_timestamp
);

------------------------------
--drop table reference.bonds_list_on_date;
create table reference.bonds_list_on_date (
 secid varchar(64),
 boardid varchar(32),
 shortname varchar(64),
 prevwaprice numeric(15,2),
 YIELDATPREVWAPRICE numeric(15,2),
 COUPONVALUE numeric(15,2),
 NEXTCOUPON varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 ACCRUEDINT numeric(15,2),
 PREVPRICE numeric(15,2),
 LOTSIZE integer,
 facevalue double precision,
 boardname varchar(512),
 status varchar(10),
 MATDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 decimals integer,
 COUPONPERIOD integer,
 issuesize bigint,
 prevlegalcloseprice numeric(15,2),
 prevdate varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 secname varchar(128),
 remarks varchar(32),
 marketcode varchar(32),
 instrid varchar(32),
 sectorid varchar(32),
 minstep double precision,
 faceunit varchar(32),
 BUYBACKPRICE numeric(15,2),
 BUYBACKDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 isin varchar(48),
 latname varchar(128), 
 regnumber varchar(128),
 currencyid varchar(32),
 ISSUESIZEPLACED bigint,
 listlevel integer,
 sectype varchar(10),
 COUPONPERCENT double precision,
 OFFERDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 settledate date,
 LOTVALUE double precision,
 FACEVALUEONSETTLEDATE double precision,
 inserttimestamp timestamp DEFAULT current_timestamp
);


--drop table reference.bonds_initial;
create table reference.bonds_initial (
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
