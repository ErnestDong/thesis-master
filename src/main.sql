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
		hr.`20-20时累计降水量` as `累计降水量`, hr.maxraining as maxraining, hr.record_date as record_date
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
		) t
	group by
		`保单号`
);

create or replace
table tmp_after engine = MergeTree PRIMARY KEY `保单号` as
(
    select
        `保单号`,
        min(`record_date`) as `record_date`
    from
        (
            select
                `保单号`,
                `record_date`
            from
                raining_impacts
            WHERE
                `保险起期` < record_date
        ) t
    group by
        `保单号`
);


create or replace
table cte_raining_impact engine = MergeTree PRIMARY KEY `保单号` as(
    select
                r.*,
                tmp.record_date as maxraining_before
    from
                raining_impacts r
    right join tmp
            on
                tmp.`record_date` = r.record_date
        and tmp.`保单号` = r.`保单号`
    order by
        `保单号`
)
--- 在时空表中去重，只取最近一天
create or replace
table raining_impact engine = MergeTree PRIMARY KEY `保单号` as(
    select
        cte.*,
        tmp2.record_date as maxraining_after
    from
        cte_raining_impact cte
    left
    join
    tmp_after tmp2
    on
        --        tmp2.`record_date` = cte.record_date
        tmp2.`保单号` = cte.`保单号`
);


alter table raining_impact  add column treated Int8 default 0;
alter table raining_impact  add column `after` Int8 default 0;

alter table raining_impact
(update
    after = 0 where 1=1);

alter table raining_impact
(update
    treated = 0 where 1=1);

--- 时间上是否受灾后：一年内发生过灾害
ALTER TABLE raining_impact
    (
    UPDATE
        after = 1,
        treated = 1
    WHERE
        record_date > (
            `保险起期` - toIntervalYear(1)
        )
);

ALTER TABLE raining_impact
    (
    UPDATE
        treated = 1
    WHERE
        maxraining_after < (
            `保险起期` + toIntervalYear(1)
        )
        and toYear(maxraining_after)!= 1970
);

alter table raining_impact
(
    update
        after = 1
    where
        treated = 0
        and record_date in (
            select
                record_date
            from
                raining_impact
            where
                treated = 1
        )
)

select treated, `after` , count(`after`), avg(`保费`) from raining_impact ri  group by treated, `after`;

select
    count(1)
from
    raining_impact ri
--group by
--    record_date
having
    record_date not in(
        select
            record_date,
        from
            raining_impact ri
        group by
            record_date
        having
            treated = 1
    );

create or replace
table ols engine = MergeTree PRIMARY KEY `保单号` as
select
    toYear(bases.`保险起期`) as t,*
from
    (
        select
            *
        from
            raining_impact ri
        where
            distance<:distance
    )cte
left join
bases
on
    cte.`保单号` = bases.`保单号`;

create or replace
view olss as(
    select
        `区站号`,
        `区站经度`,
        `区站纬度`,
        `保单号`,
        `保单经度`,
        `保单纬度`,
        `distance`,
        `保险起期`,
        `bases.保险止期` as `保险止期`,
        `保费`,
        `累计降水量`,
        `maxraining`,
        `record_date`,
        `maxraining_before`,
        `maxraining_after`,
        `treated`,
        `after`,
        `locations`,
        `bases.上年保单号` as `上年保单号`,
        `bases.保险金额` as `保险金额`,
        `bases.保费合计` as `保费合计`,
        `bases.是否涉农业务` as `是否涉农业务`,
        `bases.是否接受政府财政补贴` as `是否接受政府财政补贴`,
        `bases.保险财产购置价` as `保险财产购置价`,
        toYear(`保险起期`)  as `t`
    from
        ols o
);

select treated, `after` , count(`after`), avg(`保费`) from ols group by treated, `after` having `保费`!=0;
