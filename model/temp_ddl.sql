
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


