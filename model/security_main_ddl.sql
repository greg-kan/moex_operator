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
		

"marketdata_yields": {
	"metadata": {
		"SECID": {"type": "string", "bytes": 36, "max_size": 0},
		"BOARDID": {"type": "string", "bytes": 12, "max_size": 0},
		"PRICE": {"type": "double"},
		"YIELDDATE": {"type": "date", "bytes": 10, "max_size": 0},
		"ZCYCMOMENT": {"type": "datetime", "bytes": 19, "max_size": 0},
		"YIELDDATETYPE": {"type": "string", "bytes": 21, "max_size": 0},
		"EFFECTIVEYIELD": {"type": "double"},
		"DURATION": {"type": "int32"},
		"ZSPREADBP": {"type": "int32"},
		"GSPREADBP": {"type": "int32"},
		"WAPRICE": {"type": "double"},
		"EFFECTIVEYIELDWAPRICE": {"type": "double"},
		"DURATIONWAPRICE": {"type": "int32"},
		"IR": {"type": "double"},
		"ICPI": {"type": "double"},
		"BEI": {"type": "double"},
		"CBR": {"type": "double"},
		"YIELDTOOFFER": {"type": "double"},
		"YIELDLASTCOUPON": {"type": "double"},
		"TRADEMOMENT": {"type": "datetime", "bytes": 19, "max_size": 0},
		"SEQNUM": {"type": "int64"},
		"SYSTIME": {"type": "datetime", "bytes": 19, "max_size": 0}
	},		

select * from main.bonds_main_securities;

https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQOB/securities.json
https://iss.moex.com/iss/engines/stock/markets/bonds/boards/TQCB/securities.json
select distinct primary_boardid from reference.bonds_base;
select distinct marketprice_boardid from reference.bonds_base;

primary_boardid
marketprice_boardid

select * from history.shares_list_on_date
  where secid  = 'SBER';
