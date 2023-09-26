create external table zemtsov_raw_data 
(
	id bigint,
	date timestamp,
	channel_id integer,
	action boolean
)
location ('pxf://user/zemtsov/data/zemtsov_raw_data.parquet?PROFILE=hdfs:parquet&SERVER=hadoop')
format 'CUSTOM' (FORMATTER='pxfwritable_import');



create view zemtsov_raw_data_view (user_id, start_view, end_view, channel_id) as
select s_data.user_id, s_data.start_view, e_data.end_view, s_data.channel_id
from 
(
 select id/10000000 as user_id, id, date as start_view, channel_id
 from zemtsov_raw_data
 where action
) as s_data
inner join 
(
 select id/10000000 as user_id, id, date as end_view, channel_id
 from zemtsov_raw_data 
 where not action
) as e_data
on s_data.id = e_data.id;



create view zemtsov_data_user_view (user_id, age, gender, city, start_view, end_view, channel_id, channel_name, channel_group) as 
select users.user_id, 
       users.age,
       users.gender,
       users.city,
       users.start_view,
       users.end_view,
       users.channel_id,
       channels.name,
       channels.category
from
(
 (select * from zemtsov_raw_data_view) as raw 
 inner join
 (select id, extract(year from age(birth_date)) as age, gender, city  from zemtsov_names_dict) as names
 on raw.user_id = names.id
) as users
inner join
(
 select id, name, category from zemtsov_chan_dict
) as channels
on users.channel_id = channels.id;




create materialized view zemtsov_channel_gender_age (ages, gender, channel, channel_group, city) as 
select 
case 
	when age >=18 and age <25 then '18-25'
	when age >=25 and age <35 then '25-35'
	when age >=35 and age <45 then '35-45'
	when age >=45 and age <55 then '45-55'
	when age >=55 and age <65 then '55-65'
	when age >=65 and age <75 then '65-75'
	when age >=75 then 'over 75'
end as ages,
gender, channel_name, channel_group, city
from zemtsov_data_user_view
where extract(year from start_view)='2022' ;



create materialized view zemtsov_time_data (channel, date, start_hour, end_hour, views) as 
select 
	channel_name, 
	to_char(start_view,'yyyy-mm-dd') as date,
	count(extract(hour from start_view)) as start_hour, 
	count(extract(hour from end_view)) as end_hour,
	count(*)
from zemtsov_data_user_view
group by(channel_name,date);

drop materialized view zemtsov_time_data;
select * from zemtsov_time_data limit 10;


refresh materialized view zemtsov_channel_gender_age;
