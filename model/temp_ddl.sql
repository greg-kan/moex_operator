
SELECT secid, shortname, prevprice, prevwaprice, prevdate, prevlegalcloseprice, settledate, inserttimestamp
  FROM history.shares_list_on_date
 where secid like '%SBER%'
 order by secid, inserttimestamp;

select *
  from history.shares_history
 where secid like '%SBER%';




select *
  from ( 
select *,
       row_number() over(partition by secid order by secid, inserttimestamp desc) rn,
       count(*) over(partition by secid) cnt
  from reference.bonds_initial) a
 where a.cnt > 1;

select * from reference.bonds_base;

truncate table reference.shares_base;

select * from main.shares_main_marketdata
  where secid  = 'SBER';
 
select * from main.bonds_main_securities
 where inserttimestamp > '2024-03-10 23:59:59.000'::timestamp
  and listlevel = 1
  and shortname in ('Совком 2В3', 'ОФЗ 26233', 'Сбер Sb27R')
  and boardid in ('TQOB', 'TQCB')
 order by shortname, inserttimestamp ;

