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

