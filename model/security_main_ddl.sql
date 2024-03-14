--CREATE SCHEMA main;

--drop table main.shares_main_securities;
--truncate table main.shares_main_securities;
create table main.shares_main_securities (
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
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);

--drop table main.shares_main_marketdata;
--truncate table main.shares_main_marketdata;
create table main.shares_main_marketdata (
 secid varchar(64),
 boardid varchar(32),
 bid numeric(19,6),
 biddepth integer,
 offer numeric(19,6),
 offerdepth integer,
 spread numeric(19,6),
 biddeptht integer,
 offerdeptht integer,
 open numeric(19,6),
 low numeric(19,6),
 high numeric(19,6),
 last numeric(19,6),
 lastchange numeric(19,6),
 lastchangeprcnt numeric(19,6),
 qty integer,
 value numeric(19,6),
 value_usd numeric(19,6),
 waprice numeric(19,6),
 lastcngtolastwaprice numeric(19,6),
 waptoprevwapriceprcnt numeric(19,6),
 waptoprevwaprice numeric(19,6),
 closeprice numeric(19,6),
 marketpricetoday numeric(19,6),
 marketprice numeric(19,6),
 lasttoprevprice numeric(19,6),
 numtrades integer,
 voltoday bigint,
 valtoday bigint,
 valtoday_usd bigint,
 etfsettleprice numeric(19,6),
 tradingstatus varchar(8),
 updatetime time,
 lastbid numeric(19,6),
 lastoffer numeric(19,6),
 lcloseprice numeric(19,6),
 lcurrentprice numeric(19,6),
 marketprice2 numeric(19,6),
 numbids integer,
 numoffers integer,
 change numeric(19,6),
 "time" time,
 highbid numeric(19,6),
 lowoffer numeric(19,6),
 priceminusprevwaprice numeric(19,6),
 openperiodprice numeric(19,6),
 seqnum bigint,
 systime timestamp without time zone,
 closingauctionprice numeric(19,6),
 closingauctionvolume  numeric(19,6),
 issuecapitalization numeric(19,6),
 issuecapitalization_updatetime time,
 etfsettlecurrency varchar(32),
 valtoday_rur bigint,
 tradingsession varchar(8),
 trendissuecapitalization numeric(19,6),
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);


--truncate table main.shares_main_dataversion;
--drop table main.shares_main_dataversion;
create table main.shares_main_dataversion (
 data_version integer,
 seqnum bigint,
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);
 

--drop table main.bonds_main_securities;
--truncate table main.bonds_main_securities;
create table main.bonds_main_securities (
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
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);


--drop table main.bonds_main_marketdata;
--truncate table main.bonds_main_marketdata;
create table main.bonds_main_marketdata (
 secid varchar(64),
 bid numeric(19,6),
 biddepth integer,
 offer numeric(19,6),
 offerdepth integer,
 spread numeric(19,6),
 biddeptht integer,
 offerdeptht integer,
 open numeric(19,6),
 low numeric(19,6),
 high numeric(19,6),
 last numeric(19,6),
 lastchange numeric(19,6),
 lastchangeprcnt numeric(19,6),
 qty integer,
 value numeric(19,6),
 yield numeric(19,6),
 value_usd numeric(19,6),
 waprice numeric(19,6),
 lastcngtolastwaprice numeric(19,6),
 waptoprevwapriceprcnt numeric(19,6),
 waptoprevwaprice numeric(19,6), 
 yieldatwaprice numeric(19,6),
 yieldtoprevyield numeric(19,6),
 closeyield numeric(19,6),
 closeprice numeric(19,6),
 marketpricetoday numeric(19,6),
 marketprice numeric(19,6),
 lasttoprevprice numeric(19,6),
 numtrades integer,
 voltoday bigint,
 valtoday bigint,
 valtoday_usd bigint,
 boardid varchar(32),
 tradingstatus varchar(16),
 updatetime time,
 duration double precision,
 numbids integer,
 numoffers integer,
 change numeric(19,6),
 "time" time,
 highbid numeric(19,6),
 lowoffer numeric(19,6),
 priceminusprevwaprice numeric(19,6),
 lastbid numeric(19,6),
 lastoffer numeric(19,6),
 lcurrentprice numeric(19,6),
 lcloseprice numeric(19,6),
 marketprice2 numeric(19,6),
 openperiodprice numeric(19,6),
 seqnum bigint,
 systime timestamp,
 valtoday_rur bigint,
 iricpiclose numeric(19,6),
 beiclose numeric(19,6),
 cbrclose numeric(19,6),
 yieldtooffer numeric(19,6),
 yieldlastcoupon numeric(19,6),
 tradingsession varchar(8),
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp 
);


--truncate table main.bonds_main_dataversion;
--drop table main.bonds_main_dataversion;
create table main.bonds_main_dataversion (
 data_version integer,
 seqnum bigint,
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp
);		
		
create table main.bonds_main_marketdata_yields (
 secid varchar(64),
 boardid varchar(32),
 price numeric(19,6),
 yielddate date,
 zcycmoment timestamp,
 yielddatetype varchar(32),
 effectiveyield numeric(19,6),
 duration integer,
 zspreadbp integer,
 gspreadbp integer,
 waprice numeric(19,6),
 effectiveyieldwaprice numeric(19,6),
 durationwaprice integer,
 ir numeric(19,6),
 icpi numeric(19,6),
 bei numeric(19,6),
 cbr numeric(19,6),
 yieldtooffer numeric(19,6),
 yieldlastcoupon numeric(19,6),
 trademoment timestamp,
 seqnum bigint,
 systime timestamp,
 sess_num integer,
 inserttimestamp timestamp DEFAULT current_timestamp 
);		

