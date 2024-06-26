@set years = 20
@set duration=1
@set distance=20
@set middle=50
use thesis;
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
            `保险起期` - toIntervalYear(:duration)
        )
);

ALTER TABLE raining_impact
    (
    UPDATE
        treated = 1
    WHERE
        maxraining_after < (
            `保险起期` + toIntervalYear(:duration)
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
        AND ri.`保险起期`>hr.record_date - toIntervalYear(:duration)
        AND ri.`保险起期`<hr.record_date + toIntervalYear(
                    :duration
        )
        AND geoDistance(
                    ri.`保单经度`,
                    ri.`保单纬度`,
                    hr.`经度`,
                    hr.`纬度`
        )/ 1000<300
);

select `保单号` , post/24/60/60 as t from tmp_cte tc order by t desc;
alter table raining_impact
(
    update
        after = 1
    where
        treated = 0
        and `保单号` in (select `保单号` from tmp_cte where `post`<0)
);

select
    DISTINCT `保单号`
from
    raining_impact
where
    `after` = 1
    and treated = 0
    and `保单号` not in (
        select
            `保单号`
        from
            tmp_cte
        where post < 0
    );


select
    treated,
    `after`,
    count(`after`),
    avg(`保费`)
from
    raining_impact ri
group by
    treated,
    `after`;

select
    treated,
    `after`,
    count(`after`),
    avg(`保费`)
from
    ols_ups
group by
    treated,
    `after`
having
    `保费` != 0;

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
            and distance<:middle
            and distance>:distance
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

create view posts as(
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


create or replace
view ols_ups as(
    select
        c.total_claim as `total_claim`, b.`保单号` as `下年保单号` ,
        t.`middle` as `middle`,t.`区站号` as `区站号`,t.`区站经度` as `区站经度`,t.`区站纬度` as `区站纬度`,t.`保单号` as `保单号`,t.`保单经度` as `保单经度`,t.`保单纬度` as `保单纬度`,t.`distance` as `distance`,t.`保险起期` as `保险起期`,t.`保险止期` as `保险止期`,t.`保费` as `保费`,t.`累计降水量` as `累计降水量`,t.`maxraining` as `maxraining`,t.`record_date` as `record_date`,t.`maxraining_before` as `maxraining_before`,t.`maxraining_after` as `maxraining_after`,t.`treated` as `treated`,t.`after` as `after`,t.`locations` as `locations`,t.`上年保单号` as `上年保单号`,t.`保险金额` as `保险金额`,t.`保费合计` as `保费合计`,t.`保险财产购置价` as `保险财产购置价`,t.`建筑面积` as `建筑面积`,t.`t` as `t`,
        p.minpost as minpost, p.maxpost as maxpost,
        l.`省份`,l.`站名`
    from
        (
            select
                middle.middle as middle,
                olss.*
            from
                olss
            left join middle on
                olss.`保单号` = middle.`保单号`
        )t
    left join claims c
on
        t.`保单号` = c.`保单号`
    left join base b on b.`上年保单号` = t.`保单号`
    left join postview p on p.`保单号`=t.`保单号`
    left join location l on l.`区站号`=t.`区站号`
);

select * from ols_ups;



select
    t,
    count(t)
from
    ols_ups
group by
    t;
select count(1) from claims;
select count(1) from history_claim hc;

select * from history_claim hc left join claim on hc.`赔案号` =claim.`赔案号`;

select `赔案号`, count(`赔案号`) as cnt from history_claim group by `赔案号` order by cnt desc;

with tabx as(
    select
        record_date,
        count(1) as cnt
    from
        history_raining hr
    group by
        record_date
    having
        cnt>1
)
select
    *
from
    history_raining hr2
where
    record_date in (
        select
            record_date
        from
            tabx
    )
;

with asdf as(
    select
        *
    from
        history_raining hr,
        history_raining hr2
    where
        hr.record_date>hr2.record_date
        and hr.record_date< hr2.record_date + 30
)
select
    *,
    geoDistance(
        asdf.`经度`,
        asdf.`纬度`,
        asdf.`hr2.经度`,
        asdf.`hr2.纬度`
    )/1000 as dist
from
    asdf
order by dist;

select `treated`, `middle`,`after`,count(1) from ols_ups group by `treated`,`middle`,`after`;

select count(*) from base where hasSubsequence(`标的详细地址`, '村')=1 or hasSubsequence(`标的详细地址`, '县')=1;
select count(*) from base where hasSubsequence(`标的详细地址`, '村')=0 and hasSubsequence(`标的详细地址`, '县')=0;

select * from base where `保单号`='04FCB0F3D3616712E05400144F67CA43';
