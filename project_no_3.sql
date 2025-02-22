
create database project_NO_3;
use project_NO_3;
 
 -- ds	job_id	actor_id	event	language	time_spent	org;
 
 
create table job_data(
						ds date,
                        job_id INT not null,
                        actor_id int not null,
                        event varchar(10) not null,
                        language varchar(10) not null,
                        time_spent int not null,
                        org char(2)
                        );
                        
                        
select * from job_data;

insert into job_data(ds, job_id, actor_id, event, language, time_spent, org)
values( '2020-11-30',	21,	1001,	'skip', 'English',	15,	'A'),
('2020-11-30',	22,	1006,	'transfer',	'Arabic',	25	,'B'),
('2020-11-29'	,23,	1003,	'decision',	'Persian',	20,	'C'),
('2020-11-28',	23, 1005,	'transfer',	'Persian',	22, 'D'),
('2020-11-28',	25,	1002,	'decision',	'Hindi',	11	,'B'),
('2020-11-27',	11	,1007,	'decision',	'French',	104,	'D'),
('2020-11-26',	23	,1004,	'skip',	'Persian',	56,	'A'),
('2020-11-25',	20,	1003,	'transfer',	'Italian',	45,	'C');


#     CASE STUDY 1 ----TASK A -- JOBS REVIEWED OVER TIME
-- calculate the number of jobs reviewed per hour for each day in nov 2020 
select 
    ds as date,
    count(job_id) as total_job_id,
    round((sum(time_spent)/3600),2) as total_time_per_hour,
    round((count(job_id)/(sum(time_spent)/3600)),2) as job_review_perH_perd
    from 
		job_data
	where 
		ds between '2020-11-01' and '2020-11-30'
	group by ds
    order by ds ;
    
# CASE STUDY 1 ------THROUGHOUT ANALLYSIS----
-- calculate 7 days roliing average of throughout and daily average of throughout
select 
	round(count(event)/sum(time_spent),2) as weekly_avg_throughout
from 
	job_data;
    select 
	ds as Dates,
    round(count(event)/sum(time_spent), 2) as Daily_avg_throughout
from 
	job_data
group by ds
order by ds;



-- CASE STUDY 1 --------TASK C ----language share analysis
-- calculate the percentage share of each language over last 30 days.
select 
		language,
        round(100*count(*) / total,2) as percentage,
        jd.total
from
	job_data
		cross join 
	(select 
			count(*) as total
	from 
		 job_data) as jd
	group by language, jd.total;
    
-- identify duplicate row from the job data table 

select * from job_data;

select 
		actor_id,max(actor_id)
        from job_data
        group by actor_id
        having count(*) > 1;
        
        
use project_no_3;


drop table events;
show variables like 'secure_file_priv';
        
create table events (
user_id int,
occurred_at varchar(80),
event_type varchar(50),
event_name varchar(100),
location varchar(50),
device varchar(50),
user_type int);

desc events;

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/events.csv"
into table events
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


alter table events
modify occurred_at varchar(50);

select * from users;

alter table users add column temp_created_at datetime;

set SQL_SAFE_UPDATES = 0;

update users set temp_created_at = str_to_date(created_at, '%d-%m-%Y %H:%i');

alter table users drop column created_at;

 alter table users change column temp_created_at created_at datetime;
 
 alter table events add column temp_occurred_at datetime;
 
 select * from events;
 
 set sql_safe_updates = 0;
 
 update events set temp_occurred_at = str_to_date(occurred_at,'%d-%m-%Y %H:%i');
 
 
 alter table events drop column occurred_at;

 alter table events change column temp_occurred_at occurred_at datetime;
 
 alter table events add column temp_occurred_at datetime;

select * from email_events;


alter table email_events add column temp_occurred_at datetime;

update email_events set temp_occurred_at = str_to_date(occurred_at,'%d-%m-%Y %H:%i');
 
 
 alter table email_events drop column occurred_at;

 alter table email_events change column temp_occurred_at occurred_at datetime;
 
 select * from users;
  select * from events;
   select * from email_events;
   
alter table users add column temp_activated_at datetime;

set SQL_SAFE_UPDATES = 0;

update users set temp_activated_at = str_to_date(activated_at, '%d-%m-%Y %H:%i');

alter table users drop column activated_at;

 alter table users change column temp_activated_at activated_at datetime;
 
 alter table events add column temp_occurred_at datetime;

-- ------------ CASE STUDY 2 ------------------------
-- ---------------TASK A----------WEEKLY USER ENGAGEMENT
SELECT 
		extract(week from occurred_at) as week_log,
        count(distinct user_id) as act_users
from 
	events
where
	event_type = 'engagement'
group by week_log
order by week_log;

-- -----------TASK B--------------------
-- Write an SQL query to calculate the user growth for the product
with week_active_user as(
	select 
    extract(year from created_at) as year,
    extract(week from created_at) as week_no,
    count(distinct user_id)as num_of_users
    from users
    group by year,week_no
    )
        select 
    year,
    week_no,
    num_of_users,
    sum(num_of_users) over(order by year,week_no) as cumulative_users
from week_active_user
order by year, week_no;
    

-- ------------------ TASK C -------------------------------

select 
	first as "week_numbers",
    sum(case when week_number=0 then 1 else 0 end) as 'week_0',
    sum(case when week_number=1 then 1 else 0 end) as 'week_1',
    sum(case when week_number=2 then 1 else 0 end) as 'week_2',
    sum(case when week_number=3 then 1 else 0 end) as 'week_3',
    sum(case when week_number=4 then 1 else 0 end) as 'week_4',
    sum(case when week_number=5 then 1 else 0 end) as 'week_5',
    sum(case when week_number=6 then 1 else 0 end) as 'week_6',
    sum(case when week_number=7 then 1 else 0 end) as 'week_7',
    sum(case when week_number=8 then 1 else 0 end) as 'week_8',
    sum(case when week_number=9 then 1 else 0 end) as 'week_9',
    sum(case when week_number=10 then 1 else 0 end) as 'week_10',
    sum(case when week_number=11 then 1 else 0 end) as 'week_11',
    sum(case when week_number=12 then 1 else 0 end) as 'week_12',
    sum(case when week_number=13 then 1 else 0 end) as 'week_13',
    sum(case when week_number=14 then 1 else 0 end) as 'week_14',
    sum(case when week_number=15 then 1 else 0 end) as 'week_15',
    sum(case when week_number=16 then 1 else 0 end) as 'week_16',
    sum(case when week_number=17 then 1 else 0 end) as 'week_17',
    sum(case when week_number=18 then 1 else 0 end) as 'week_18'
from (
    select 
		m.user_id,
        m.login_week,
        n.first,
        m.login_week - n.first as week_number
        from (
			select 
            user_id,
            extract(week from occurred_at) as login_week
		from 
        events
        group by 
        user_id, login_week
        ) m
        join (
        select 
        user_id,
        min(extract(week from occurred_at )) as first 
        from 
			events
		group by 
        user_id
        ) n 
        on m.user_id = n.user_id 
        ) sub
        group by first 
        order by first;
    
    
-- ------------------- CASE STUDY 2 ----TASK D ------------WEEKLY ENGAGEMENT PER DEVICE;

SELECT 
		extract(week from occurred_at) as week_number,
		count(distinct case when device = 'dell inspiron notebook' then user_id else null end ) as dell_inspiron_notebook,
        count(distinct case when device = 'iphone 5' then user_id else null end ) as iphone_5,
        count(distinct case when device = 'iphone 4s' then user_id else null end ) as iphone_4s,
        count(distinct case when device = 'iphone 5s' then user_id else null end ) as iphone_5s,
        count(distinct case when device = 'ipad air' then user_id else null end ) as ipad_air,
        count(distinct case when device = 'windows surface' then user_id else null end ) as window_surface,
        count(distinct case when device = 'macbook air' then user_id else null end ) as macbook_air,
        count(distinct case when device = 'macbook pro' then user_id else null end ) as macbook_pro,
        count(distinct case when device = 'ipad mini' then user_id else null end ) as ipad_mini,
        count(distinct case when device = 'kindle fire' then user_id else null end ) as kindle_fire,
        count(distinct case when device = 'amazon fire phone' then user_id else null end ) as amazon_fire_phone,
        count(distinct case when device = 'nexus 5' then user_id else null end ) as nexus_5,
        count(distinct case when device = 'nexus 7' then user_id else null end ) as nexus_7,
        count(distinct case when device = 'nexus 10' then user_id else null end ) as nexus_10,
        count(distinct case when device = 'samsung galaxy s4' then user_id else null end ) as samsung_galaxy_s4,
        count(distinct case when device = 'samsung galaxy tablet' then user_id else null end ) as samsung_galaxy_tablet,
        count(distinct case when device = 'samsung galaxy note' then user_id else null end ) as samsung_galaxy_note,
        count(distinct case when device = 'lenovo thinkpad' then user_id else null end ) as lenovo_thinkpad,
        count(distinct case when device = 'acer aspire notebook' then user_id else null end ) as acer_aspire_notebook,
        count(distinct case when device = 'asus chromebook' then user_id else null end ) as asus_chromebook,
        count(distinct case when device = 'htc one' then user_id else null end ) as htc_one,
        count(distinct case when device = 'nokia lumnia 635' then user_id else null end ) as nokia_lumnia_635,
        count(distinct case when device = 'mac mini' then user_id else null end ) as mac_mini,
        count(distinct case when device = 'hp pavilion desktop' then user_id else null end ) as hp_pavilion_desktop,
        count(distinct case when device = 'dell inspiron desktop' then user_id else null end ) as dell_inspiron_desktop
        from 
			events
		where
			event_type = "engagement"
		group by week_number
        order by week_number;
        
        
        
        
-- -------------------------- TASK E -------------------------------------EMAIL ENGAGEMENT ANALYSIS

SELECT 
	100.0*SUM(CASE WHEN email_action= 'email_open' then 1 else 0 end)/
    sum(case when email_action = 'email_sent' then 1 else 0 end) as email_open_rate,
    
    100.0*SUM(CASE WHEN email_action= 'email_clicked' then 1 else 0 end)/
    sum(case when email_action = 'email_sent' then 1 else 0 end) as email_clicked_rate
    from 
    (select *,
			case
				when action in ('sent_weekly_digest','sent_reengagement_email') then 'email_sent'
                when action in ('email_open') then 'email_open'
                when action in ('email_clickthrough') then 'email_clicked'
                else null
			end as email_action
		from 
			project_no_3.email_events
	) a ;	






