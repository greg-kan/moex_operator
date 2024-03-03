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
