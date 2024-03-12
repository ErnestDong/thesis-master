@set years = 20
@set duration=60
@set distance=20
use thesis;


--create database thesis;

--- 补充气象站地理位置：经纬度有错的按top1频率
create or replace
view locations as
select
	`区站号`,
	arrayElement(topK(1)(`经度`),1)/100 as `经度`,
	arrayElement(topK(1)(`纬度`),1)/100 as `纬度`
from
	raining r
group by
	`区站号`;


-- 降水join地区
create or replace
view rainings as
select
	makeDate32(
		r.`年`,
		r.`月`,
		r.`日`
	) as "record_date",
	r.`区站号`,
	r.`20-20时累计降水量`,
	l.`经度` as `经度`,
	l.`纬度` as `纬度`
from
	raining r
left join
	locations l
on
	r.`区站号` = l.`区站号`
where
	r.`年`>1994 and r.`20-20时累计降水量`<30000;

-- 删除缺乏地理位置和过时的保单
CREATE or replace
view bases as
select
	geohashEncode(
		`标的经度`,
		`标的纬度`
	) as locations ,
	*
FROM
	base
where
	`标的经度` != 0
	and `保险起期` < makeDate32(
		2019,
		10,
		1
	)
	and `保险起期`> makeDate32(
		1994,
		12,
		31
	);

--- x年一遇的大暴雨
create or replace
view history_raining as
with CTE as (
	select
			`区站号`,
			quantile(
				1-1 / :years / 365
		)(`20-20时累计降水量`) as maxraining
	from
			rainings r
	group by
			`区站号`
)
select
	geohashEncode(
		r.`经度`,
		r.`纬度`
	) as locations,
	r.`经度` ,
	r.`纬度` ,
	r.record_date,
	r.`区站号`,
	r.`20-20时累计降水量`,
	CTE.maxraining
from
	rainings r
left join
CTE
on
	r.`区站号` = CTE.`区站号`
where
	r.`20-20时累计降水量` >= CTE.maxraining
order by
	r.`区站号`,
	r.record_date;

--- 区站x标的的笛卡尔积
CREATE or replace
view forJoin as
(
	select
		l.`区站号` as `区站号`,
		l.`经度` as `区站经度`,
		l.`纬度` as `区站纬度`,
		b.`保单号` as `保单号`,
		b.`标的经度` as `保单经度`,
		b.`标的纬度` as `保单纬度`,
		greatCircleDistance(
			`区站经度`,
			`区站纬度`,
			`保单经度`,
			`保单纬度`
		)/ 1000 as distance
	from
		locations l,
		bases b
);

--- 云上得到距离保险标的最近的气象站
--- clickhouse client --host simfv776a0.ap-south-1.aws.clickhouse.cloud --secure --password "Z4cfvnADI5MM."

CREATE TABLE thesis.res (
    `区站号` Int32,
    `区站经度` Float64,
    `区站纬度` Float64,
    `保单号` String,
    `保单经度` Float64,
    `保单纬度` Float64,
    `distance` Float64
) ENGINE = MergeTree PRIMARY KEY (`保单号`, `区站号`)
ORDER BY
    (`保单号`, `区站号`) SETTINGS index_granularity = 8192;
select
	*
from
	forJoin b
right join
(
		select
			`保单号`,
			min(distance) as distance
		FROM
			 forJoin
		group by
			`保单号`
	) subquery
on
	subquery.`保单号` = b.`保单号`
	and subquery.distance = b.distance;
insert
	into
	res
select
	b.`区站号`,
	b.`区站经度`,
	b.`区站纬度`,
	b.`保单号`,
	b.`保单经度`,
	b.`保单纬度` ,
	distance
from
	forJoin b
right join
(
		select
			`保单号`,
			min(distance) as distance
		FROM
			 forJoin
		group by
			`保单号`
	) subquery
on
	subquery.`保单号` = b.`保单号`
	and subquery.distance = b.distance
;

delete from res where `保单号`='';
--- 计算

--- 在空间表中加入保单时间信息
create or replace
view results as
select
	res.*, bases.`保险起期` as `保险起期`, bases.`保费合计` as `保费`
from
	res
join
	bases
on
	res.`保单号` = bases.`保单号`;

--- 在时空表中加入暴雨时间信息
create or replace view raining_impacts as(
	select
		r.*,
		hr.*
	from
		results r
	left join
	history_raining hr
	on
		r.`区站号` = hr.`区站号`
);
select * from raining_impacts ri limit 10;

--- 连续多天为x年大暴雨的情况下，取最近的一天
create or replace
table tmp engine = MergeTree PRIMARY KEY `保单号` as
(
	select
		`保单号`,
		max(`record_date`) as `record_date`
	from
		(
			select
				`保单号`,
				`record_date`
			from
				raining_impacts
			WHERE
				`保险起期` > record_date
--				and `保险起期` < record_date + 365
		) t
	group by
		`保单号`
) ;

select * from tmp;

select
    *
from
    (
        select
            *
        from
            tmp
        limit 20
    ) t
left join base
on
    t.`保单号` = base.`保单号`;

--- 在时空表中去重，只取最近一天
create or replace
table raining_impact engine = MergeTree PRIMARY KEY `保单号` as(
    select
        *
    from
        raining_impacts r
    right join tmp
on
        tmp.`record_date` = r.record_date
        and tmp.`保单号` = r.`保单号`
);


select
	(
		select
			count(1)
		from
			res
		where distance < :distance
	) as res_count,
	(
		select
			count(1)
		from
			raining_impact
        where distance < :distance
	) as raining_impact_count;

select
    *
from
    raining_impact
where
    distance < :distance;

--select
--    *
--from
--    base b
--where
--    geoDistance(
--        b.`标的经度`,
--        b.`标的纬度`,
--        108.24,
--        30.46
--    )<:distance*1000
--order by base.`保险起期`;
--
--select
--    *
--from
--    history_raining hr
--where
--    hr.`区站号` = 57432;
--
--select
--    r.`20-20时累计降水量`,
--    count(r.`20-20时累计降水量`) as cnt
--from
--    raining r
--where
--    r.`区站号` = 57432
--group by
--    `20-20时累计降水量`
--order by cnt desc;
--
--select
--    `20-20时累计降水量` as rain,
--    count(`20-20时累计降水量`) as cnt
--from
--    raining r
--group by
--    `20-20时累计降水量`
--order by
--    rain desc;
--
--select * from res;
--select
--    count(1)
--from
--    bases b
--where
--    b.`上年保单号` in (
--        select
--            `保单号`
--        from
--            raining_impact ri
--    );

--select
--    toYear(`保险起期`) as year_num,
--    count(year_num) as cnt
--from
--    raining_impact ri
--group by
--    year_num
--having
--    distance < :distance;

create or replace
table ols engine = MergeTree() PRIMARY KEY `保单号` as(
    select
        cte.*,
        ri.record_date,
        ri.`hr.20-20时累计降水量`
    from
        (
            select
                res.`区站号`,
                res.distance,
                bases.*
            from
                bases
            left join
res
on
                bases.`保单号` = res.`保单号`
        ) cte
    left join
raining_impact ri
on
        cte.`保单号` = ri.`保单号`
);

alter table ols add column treated Int8 default 0;
alter table ols add column `after` Int8 default 0;
update ols set `after` = 1 where record_date > `保险起期` + 365;

select record_date, toDate32(`保险起期`) from ols limit 20;
---
select * from ols limit 20;

select count(*) from ols where `after`=1;
