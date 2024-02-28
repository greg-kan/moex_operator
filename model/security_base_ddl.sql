create schema history;
CREATE SCHEMA reference;

--truncate table history.stock_shares_securities_history;
create table history.stock_shares_securities_history (
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
 inserttimestamp timestamp DEFAULT current_timestamp
);


--truncate table history.stock_bonds_securities_history;
create table history.stock_bonds_securities_history (
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
 inserttimestamp timestamp DEFAULT current_timestamp
);


--truncate table history.stock_shares_securities_history_2023_2023;
create table history.stock_shares_securities_history_2023_2023 (
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
 trendclspr numeric(19,6)
);


--drop table history.shares_list_on_date;
--truncate table history.shares_list_on_date;
create table history.shares_list_on_date (
 secid varchar(64),
 boardid varchar(32),
 shortname varchar(256),
 prevprice numeric(19,6),
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
 prevwaprice numeric(19,6),
 faceunit varchar(32),
 prevdate varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 issuesize bigint,
 isin varchar(48),
 latname varchar(128),
 regnumber varchar(128),
 prevlegalcloseprice numeric(19,6),
 currencyid varchar(32),
 sectype varchar(10),
 listlevel integer,
 settledate date,
 inserttimestamp timestamp DEFAULT current_timestamp
);


--drop table reference.bonds_list_on_date;
--truncate table reference.bonds_list_on_date;
create table reference.bonds_list_on_date (
 secid varchar(64),
 boardid varchar(32),
 shortname varchar(64),
 prevwaprice numeric(19,6),
 YIELDATPREVWAPRICE numeric(19,6),
 COUPONVALUE numeric(19,6),
 NEXTCOUPON varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 ACCRUEDINT numeric(19,6),
 PREVPRICE numeric(19,6),
 LOTSIZE integer,
 facevalue double precision,
 boardname varchar(512),
 status varchar(10),
 MATDATE varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 decimals integer,
 COUPONPERIOD integer,
 issuesize bigint,
 prevlegalcloseprice numeric(19,6),
 prevdate varchar(16), ---- recreate with type date and fix tis dates "0000-00-00"
 secname varchar(128),
 remarks varchar(32),
 marketcode varchar(32),
 instrid varchar(32),
 sectorid varchar(32),
 minstep double precision,
 faceunit varchar(32),
 BUYBACKPRICE numeric(19,6),
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

--drop table reference.shares_base;
--truncate table reference.shares_base;
create table reference.shares_base (
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

