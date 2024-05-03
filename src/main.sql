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
		r.`年`,
	l.`经度` as `经度`,
	l.`纬度` as `纬度`
from
	raining r
left join
	locations l
on
	r.`区站号` = l.`区站号`
where
	r.`20-20时累计降水量`<30000;

-- 删除缺乏地理位置和过时的保单
CREATE or replace
view bases as
select
	geohashEncode(
		`标的经度`,
		`标的纬度`
	) as locations ,
	toDate32(`保险起期`) AS `保险起期`,
	toDate32(`保险止期`) AS `保险止期`,
	* EXCEPT(`保险起期`,`保险止期`),
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
				1-1 / 20 / 365
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
	r.`年`,
	CTE.maxraining
from
	rainings r
left join
CTE
on
	r.`区站号` = CTE.`区站号`
where
	r.`20-20时累计降水量` >= CTE.maxraining and r.`年`>1994
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
);
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

alter table raining_impact drop column if exists treated;
alter table raining_impact drop column if exists `after`;
alter table raining_impact add column treated Int8 default 0;
alter table raining_impact add column `after` Int8 default 0;

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

CREATE OR replace
TABLE tmp_cte engine =MergeTree primary key `保单号` AS (
    SELECT
                `保单号`,
                timediff(
                    ri.`保险起期`,
                    hr.record_date
        ) AS `post`
    FROM
                raining_impact ri,
                history_raining hr
    WHERE
                ri.treated = 0
        AND ri.`保险起期`>hr.record_date - toIntervalYear(1)
        AND ri.`保险起期`<hr.record_date + toIntervalYear(1)
        AND geoDistance(
                    ri.`保单经度`,
                    ri.`保单纬度`,
                    hr.`经度`,
                    hr.`纬度`
        )/ 1000<300
);

select `保单号` , post/24/60/60 as t from tmp_cte tc order by t asc;

alter table raining_impact
(
    update
        after = 1
    where
        treated = 0
        and `保单号` in (select `保单号` from tmp_cte where `post`<0)
);
select ri.treated ,ri.`after`, count(1) from raining_impact ri group by ri.treated ,ri.`after` ;
create or replace
table ols engine = MergeTree PRIMARY KEY `保单号` as
select
    toYear(
        bases.`保险起期`
    ) as t,
    *
from
    (
        select
            *
        from
            raining_impact ri
        where
            distance<20
    )cte
left join
bases
on
    cte.`保单号` = bases.`保单号`;
create or replace
table ols engine = MergeTree PRIMARY KEY `保单号` as
select
    toYear(
        bases.`保险起期`
    ) as t,
    *
from
    (
        select
            *
        from
            raining_impact ri
        where
            distance<20
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
        `保险止期`,
        `保费`,
        `累计降水量`,
        `maxraining`,
        `record_date`,
        `maxraining_before`,
        `maxraining_after`,
        `treated`,
        `after`,
        `locations`,
        `上年保单号`,
        `保险金额`,
        `保费合计`,
        `保险财产购置价`,
        `建筑面积`,
        toYear(`保险起期`) as `t`
    from
        ols o
);

select * from ols;

create view claims as
select
    `保单号`,
    sum(`累计赔付金额`) as total_claim
from
    claim
group by
    `保单号`
having
    `保单号` != ''
order by
    total_claim desc;

select DISTINCT count(1) from claims join bases on claims.`保单号` =bases.`上年保单号`;
select DISTINCT count(1) from claims;
create or replace
table near engine = MergeTree primary key `保单号` as
select
    *
from
    olss
where
    treated = 0;

create or replace
table middle engine = MergeTree primary key `保单号` as
with cte1 as(
    select
        geoDistance(
            cte.`保单经度`,
            cte.`保单纬度`,
            hr.`经度`,
            hr.`纬度`
        )/ 1000 as distance,
        *
    from
        (
            select
                `保单号`,
                `保单经度`,
                `保单纬度`,
                `保险起期`
            from
                near n
        ) cte,
    (
            select
                `经度`,
                `纬度`,
                `record_date`,
                `区站号`
            from
                history_raining
        ) hr
    where
        cte.`保险起期`>hr.`record_date` - toIntervalYear(1)
        and cte.`保险起期` < hr.`record_date` + toIntervalYear(1)
            and distance<50
            and distance>20
)
select
    cte1.*
from
    cte1
inner join(
        select
            `保单号`,
            max(record_date) as record_date
        from
            cte1
        group by
            `保单号`
    ) b
on
    cte1.`保单号` = b.`保单号`
    and cte1.record_date = b.record_date;

alter table middle add column middle Int8 default 1;

create or replace view posts as(
    select
            `保单号`,
            min(post)/ 24 / 60 / 60 as minpost
    from
            tmp_cte tc
    where
            tc.post > 0
    group by
            `保单号`
);
create or replace
view postview as(
    select
        posts.*,
        t.maxpost
    from
        posts
    outer join
(
            select
                `保单号`,
                max(post)/ 24 / 60 / 60 as maxpost
            from
                tmp_cte tc
            where
                tc.post < 0
            group by
                `保单号`
        )t
            using `保单号`
);

select * from postview;
select * from olss;

CREATE OR replace
TABLE ols_with_mid engine = MergeTree PRIMARY KEY `保单号` AS
	SELECT
		middle.middle AS middle,
		olss.*
FROM
		olss
LEFT JOIN middle ON
		olss.`保单号` = middle.`保单号`;


CREATE OR replace
TABLE ols_with_next engine = MergeTree PRIMARY KEY `保单号` AS
SELECT
	b.`保单号` AS `下年保单号`,
	owm.*
FROM
	ols_with_mid owm
LEFT JOIN
(
	SELECT
		`上年保单号`,
		`保单号`
	FROM
		base b
	WHERE
		`上年保单号`!= '') b ON
	owm.`保单号`= b.`上年保单号`;

	--        c.total_claim as `total_claim`,
	--        p.minpost as minpost, p.maxpost as maxpost,
	--        l.`省份`,l.`站名`

CREATE OR replace
TABLE ols_with_claim engine = MergeTree PRIMARY KEY `保单号` AS
SELECT
	c.total_claim AS `total_claim`,
	own.*
FROM
	ols_with_next own
LEFT JOIN claims c using `保单号`;
CREATE OR replace
TABLE ols_with_post engine = MergeTree PRIMARY KEY `保单号` AS
SELECT
	p.minpost AS minpost,
	p.maxpost AS maxpost,
	own.*
FROM
	ols_with_claim own
LEFT JOIN postview p using `保单号`;

CREATE OR replace
TABLE ols_with_loc engine = MergeTree PRIMARY KEY `保单号` AS
SELECT
	l.`省份`,
	l.`站名`,
	own.*
FROM
	ols_with_post own
LEFT JOIN location l using `区站号`;

-- tosql2.py
CREATE or replace TABLE ols_up ENGINE=MergeTree PRIMARY KEY `保单号` AS(
SELECT
	ou.*,
	g.`gdp`, g.`保险密度`, g.`保险深度`
FROM
	ols_with_loc ou
LEFT JOIN
gdp g
ON
	ou.`省份` = g.province
	AND ou.t = g.`year`
);

create view ols_ups as select * from ols_up ou where `保单号`!='';
select treated, middle, `after`, avg(`保险金额`), count(1) from ols_up o where `保单号`!='' group by treated ,middle, `after`;
select min(`保险深度`),max(`保险深度`) from ols_up;
select count(1) from ols_up where `累计降水量`=0;




select treated , middle , `after` ,COUNT(1) from ols_up ou where minpost!=0 or maxpost !=0 group by treated , middle , `after`;