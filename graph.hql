drop table Graph;

create table Graph(
vin int,
vout int)
row format delimited fields terminated by ',' stored as textfile;

load data local inpath '${hiveconf:G}' overwrite into table Graph;

select vout,count(vout) as incoming from Graph
group by vout
order by incoming desc;
