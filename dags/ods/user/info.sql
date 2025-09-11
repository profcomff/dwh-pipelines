create temporary table temp_combined_data as
with temp_source_data as(
	select id, name from "STG_USERDATA".source
),
temp_stg_userdata_data as(
	select
	  owner_id as user_id,
	  string_agg(distinct case when p.name = 'Электронная почта' then value end, ', ') as email,
	  string_agg(distinct case when p.name = 'Электронная почта' then temp_source_data.name end, ', ') as email_source,
	  string_agg(distinct case when p.name = 'Номер телефона' then value end, ', ') as phone_number,
	  string_agg(distinct case when p.name = 'Номер телефона' then temp_source_data.name end, ', ') as phone_number_source,
	  string_agg(distinct case when p.name = 'Имя пользователя VK' then value end, ', ') as vk_username,
	  string_agg(distinct case when p.name = 'Имя пользователя VK' then temp_source_data.name end, ', ') as vk_username_source,
	  string_agg(distinct case when p.name = 'Город' then value end, ', ') as city,
	  string_agg(distinct case when p.name = 'Город' then temp_source_data.name end, ', ') as city_source,
	  string_agg(distinct case when p.name = 'Родной город' then value end, ', ') as birth_city,
	  string_agg(distinct case when p.name = 'Родной город' then temp_source_data.name end, ', ') as birth_city_source,
	  string_agg(distinct case when p.name = 'Место жительства' then value end, ', ') as address,
	  string_agg(distinct case when p.name = 'Место жительства' then temp_source_data.name end, ', ') as address_source,
	  string_agg(distinct case when p.name = 'Имя пользователя GitHub' then value end, ', ') as git_hub_username,
	  string_agg(distinct case when p.name = 'Имя пользователя GitHub' then temp_source_data.name end, ', ') as git_hub_username_source,
	  string_agg(distinct case when p.name = 'Имя пользователя Telegram' then value end, ', ') as telegram_username,
	  string_agg(distinct case when p.name = 'Имя пользователя Telegram' then temp_source_data.name end, ', ') as telegram_username_source,
	  string_agg(distinct case when p.name = 'Домашний номер телефона' then value end, ', ') as home_phone_number,
	  string_agg(distinct case when p.name = 'Домашний номер телефона' then temp_source_data.name end, ', ') as home_phone_number_source,
	  string_agg(distinct case when p.name = 'Ступень обучения' then value end, ', ') as education_level,
	  string_agg(distinct case when p.name = 'Ступень обучения' then temp_source_data.name end, ', ') as education_level_source,
	  string_agg(distinct case when p.name = 'ВУЗ' then value end, ', ') as university,
	  string_agg(distinct case when p.name = 'ВУЗ' then temp_source_data.name end, ', ') as university_source,
	  string_agg(distinct case when p.name = 'Факультет' then value end, ', ') as faculty,
	  string_agg(distinct case when p.name = 'Факультет' then temp_source_data.name end, ', ') as faculty_source,
	  string_agg(distinct case when p.name = 'Академическая группа' then value end, ', ') as academic_group,
	  string_agg(distinct case when p.name = 'Академическая группа' then temp_source_data.name end, ', ') as academic_group_source,
	  string_agg(distinct case when p.name = 'Должность' then value end, ', ') as position,
	  string_agg(distinct case when p.name = 'Должность' then temp_source_data.name end, ', ') as position_source,
	  string_agg(distinct case when p.name = 'Номер студенческого билета' then value end, ', ') as student_id,
	  string_agg(distinct case when p.name = 'Номер студенческого билета' then temp_source_data.name end, ', ') as student_id_source,
	  string_agg(distinct case when p.name = 'Кафедра' then value end, ', ') as department,
	  string_agg(distinct case when p.name = 'Кафедра' then temp_source_data.name end, ', ') as department_source,
	  string_agg(distinct case when p.name = 'Форма обучения' then value end, ', ') as education_form,
	  string_agg(distinct case when p.name = 'Форма обучения' then temp_source_data.name end, ', ') as education_form_source,
	  string_agg(distinct case when p.name = 'Полное имя' then value end, ', ') as full_name,
	  string_agg(distinct case when p.name = 'Полное имя' then temp_source_data.name end, ', ') as full_name_source,
	  string_agg(distinct case when p.name = 'Дата рождения' then value end, ', ') as birthday,
	  string_agg(distinct case when p.name = 'Дата рождения' then temp_source_data.name end, ', ') as birthday_source,
	  string_agg(distinct case when p.name = 'Фото' then value end, ', ') as photo,
	  string_agg(distinct case when p.name = 'Фото' then temp_source_data.name end, ', ') as photo_source,
	  string_agg(distinct case when p.name = 'Пол' then value end, ', ') as sex,
	  string_agg(distinct case when p.name = 'Пол' then temp_source_data.name end, ', ') as sex_source,
	  string_agg(distinct case when p.name = 'Место работы' then value end, ', ') as workplace,
	  string_agg(distinct case when p.name = 'Место работы' then temp_source_data.name end, ', ') as workplace_source,
	  string_agg(distinct case when p.name = 'Расположение работы' then value end, ', ') as workplace_address,
	  string_agg(distinct case when p.name = 'Расположение работы' then temp_source_data.name end, ', ') as workplace_address_source
	from "STG_USERDATA".{{ params.tablename }} i
	left join "STG_USERDATA".param p on i.param_id = p.id
	join temp_source_data on i.source_id = temp_source_data.id
	group by owner_id
),
temp_stg_union_member_data as(
	select 
		type_of_learning as education_form,
		rzd_status as rzd_status,
		academic_level as education_level,
		status as status,
		faculty as faculty,
		email as email,
		date_of_birth as birthday,
		phone_number as phone_number,
		image as photo,
		rzd_datetime as rzd_datetime,
		rzd_number as rzd_number,
		grade_level as grade_level,
		has_student_id as has_student_id,
		entry_date as entry_date,
		status_gain_date as status_gain_date,
		card_id as card_id,
		card_status as card_status,
		card_date as card_date,
		card_number as card_number,
		card_user as card_user,
		student_id as student_id,
		CONCAT_WS(' ',first_name, middle_name, last_name) as full_name,
		'union_member' as source
	from "STG_UNION_MEMBER".union_member
)
select
		ud.user_id as user_id,
		ud.academic_group as academic_group,
		ud.academic_group_source as academic_group_source,
		ud.address as address,
		ud.address_source as address_source,
		ud.birth_city as birth_city,
		ud.birth_city_source as birth_city_source,
		COALESCE(
		    CASE WHEN ud.birthday ~ '^\d{2}-\d{2}-\d{4}' THEN ud.birthday::TIMESTAMP ELSE NULL END,
		    CASE WHEN um.birthday ~ '^\d{2}-\d{2}-\d{4}' THEN um.birthday::TIMESTAMP ELSE NULL END
		) AS birthday,
	    CASE 
	        WHEN um.birthday IS NOT NULL THEN um.source
	        WHEN ud.birthday IS NOT NULL THEN ud.birthday_source
	    END AS birthday_source,
	    ud.city as city,
	    ud.city_source as city_source,
	    ud.department as department,
	    ud.department_source as department_source,
	    CONCAT_WS(', ',um.education_form, ud.education_form) as education_form,
	    case
	    	when um.education_form is not null and ud.education_form is not null then CONCAT(um.source, ', ', ud.education_form_source)
	    	when um.education_form is not null then um.source
	    	when ud.education_form is not null then ud.education_form_source
	    end  as education_form_source,
	   CONCAT_WS(', ', um.education_level, ud.education_level)  as education_level,
	    case
	    	when um.education_level is not null and ud.education_level is not null then CONCAT(um.source, ', ', ud.education_level_source)
	    	when um.education_level is not null then um.source
	    	when ud.education_level is not null then ud.education_level_source
	    end  as education_level_source,
	    CONCAT_WS(', ', um.email, ud.email) as email,
	    case
	    	when um.email is not null and ud.email is not null then CONCAT(um.source, ', ', ud.email_source)
	    	when um.email is not null then um.source
	    	when ud.email is not null then ud.email_source
	    end  as email_source,
	    CONCAT_WS(', ', um.faculty, ud.faculty) as faculty,
	    case
	    	when um.faculty is not null and ud.faculty is not null then CONCAT(um.source, ', ', ud.faculty_source)
	    	when um.faculty is not null then um.source
	    	when ud.faculty is not null then ud.faculty_source
	    end  as faculty_source,
	    CONCAT_WS(', ', um.full_name, ud.full_name) as full_name,
	    case
	    	when um.full_name is not null and ud.full_name is not null then CONCAT(um.source, ', ', ud.full_name_source)
	    	when um.full_name is not null then um.source
	    	when ud.full_name is not null then ud.full_name_source
	    end  as full_name_source,
	    ud.git_hub_username as git_hub_username,
	    ud.git_hub_username_source as git_hub_username_source,
	    ud.home_phone_number as home_phone_number,
	    ud.home_phone_number_source as home_phone_number_source,
	   CONCAT_WS(', ', um.phone_number, ud.phone_number) as phone_number,
	    case
	    	when um.phone_number is not null and ud.phone_number is not null then CONCAT(um.source, ', ', ud.phone_number_source)
	    	when um.phone_number is not null then um.source
	    	when ud.phone_number is not null then ud.phone_number_source
	    end  as phone_number_source,
	    CONCAT_WS(', ', um.photo, ud.photo) as photo,
	    case
	    	when um.photo is not null and ud.photo is not null then CONCAT(um.source, ', ', ud.photo_source)
	    	when um.photo is not null then um.source
	    	when ud.photo is not null then ud.photo_source
	    end  as photo_source,
	    ud.position as position,
	    ud.position_source as position_source,
	    ud.sex as sex,
	    ud.sex_source as sex_source,
	    CONCAT_WS(', ', um.student_id, ud.student_id) as student_id,
	    case
	    	when um.student_id is not null and ud.student_id is not null then CONCAT(um.source, ', ', ud.student_id_source)
	    	when um.student_id is not null then um.source
	    	when ud.student_id is not null then ud.student_id_source
	    end  as student_id_source,
	    ud.telegram_username as telegram_username,
	    ud.telegram_username_source as telegram_username_source,
	    ud.university as university,
	    ud.university_source as university_source,
	    ud.vk_username as vk_username,
	    ud.vk_username_source as vk_username_source,
	    ud.workplace as workplace,
	    ud.workplace_source as workplace_source,
	    ud.workplace_address as workplace_address,
	    ud.workplace_address_source as workplace_address_source,
	    um.status as status,
	    um.source as status_source,
	    um.status_gain_date as status_gain_date,
	    um.rzd_number as rzd_number,
	    um.source as rzd_number_source,
	    um.rzd_status as rzd_status,
	    um.rzd_datetime as rzd_datetime  
	from temp_stg_userdata_data ud full outer join temp_stg_union_member_data um on ud.student_id = um.student_id;

insert into "ODS_USERDATA".academic_group (
	"group", 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	academic_group,
	user_id,
	academic_group_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where academic_group is not null
on conflict(user_id, "group") do update set
	"group" = EXCLUDED."group",
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".address (
	address, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	address,
	user_id,
	address_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where address is not null
on conflict(user_id, address) do update set
	address = EXCLUDED.address,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".birth_city(
	city, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	birth_city,
	user_id,
	birth_city_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where birth_city is not null
on conflict(user_id, city) do update set
	city = EXCLUDED.city,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".birthday(
	birthday, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	birthday,
	user_id,
	birthday_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where birthday is not null
on conflict(user_id, birthday) do update set
	birthday = EXCLUDED.birthday,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".city(
	city, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	city,
	user_id,
	city_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where city is not null
on conflict(user_id, city) do update set
	city = EXCLUDED.city,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".department(
	department, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	department,
	user_id,
	department_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where department is not null
on conflict(user_id, department) do update set
	department = EXCLUDED.department,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".education_form(
	form, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	education_form,
	user_id,
	education_form_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where education_form is not null
on conflict(user_id, form) do update set
	form = EXCLUDED.form,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".education_level(
	level, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	education_level,
	user_id,
	education_level_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where education_level is not null
on conflict(user_id, level) do update set
	level = EXCLUDED.level,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".email(
	email, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	email,
	user_id,
	email_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where email is not null
on conflict(user_id, email) do update set
	email = EXCLUDED.email,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".faculty(
	faculty, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	faculty,
	user_id,
	faculty_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where faculty is not null
on conflict(user_id, faculty) do update set
	faculty = EXCLUDED.faculty,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".full_name(
	name, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	full_name,
	user_id,
	full_name_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where full_name is not null
on conflict(user_id, name) do update set
	name = EXCLUDED.name,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".git_hub_username(
	username, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	git_hub_username,
	user_id,
	git_hub_username_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where git_hub_username is not null
on conflict(user_id, username) do update set
	username = EXCLUDED.username,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".home_phone_number(
	phone_number, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	home_phone_number,
	user_id,
	home_phone_number_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where home_phone_number is not null
on conflict(user_id, phone_number) do update set
	phone_number = EXCLUDED.phone_number,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".phone_number(
	phone_number, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	phone_number,
	user_id,
	phone_number_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where phone_number is not null
on conflict(user_id, phone_number) do update set
	phone_number = EXCLUDED.phone_number,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".photo(
	url, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	photo,
	user_id,
	photo_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where photo is not null
on conflict(user_id, url) do update set
	url = EXCLUDED.url,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".position(
	position, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	position,
	user_id,
	position_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where position is not null
on conflict(user_id, position) do update set
	position = EXCLUDED.position,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".sex(
	gender, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	sex,
	user_id,
	sex_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where sex is not null
on conflict(user_id, gender) do update set
	gender = EXCLUDED.gender,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".student_id(
	student_id, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	student_id,
	user_id,
	student_id_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where student_id is not null
on conflict(user_id, student_id) do update set
	student_id = EXCLUDED.student_id,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".telegram_username(
	username, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	telegram_username,
	user_id,
	telegram_username_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where telegram_username is not null
on conflict(user_id, username) do update set
	username = EXCLUDED.username,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".university(
	university, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	university,
	user_id,
	university_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where university is not null
on conflict(user_id, university) do update set
	university = EXCLUDED.university,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;

insert into "ODS_USERDATA".vk_username(
	username, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	vk_username,
	user_id,
	vk_username_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where vk_username is not null
on conflict(user_id, username) do update set
	username = EXCLUDED.username,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".workplace(
	workplace, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	workplace,
	user_id,
	workplace_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where workplace is not null
on conflict(user_id, workplace) do update set
	workplace = EXCLUDED.workplace,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".workplace_address(
	address, 
	user_id, 
	source, 
	created, 
	modified, 
	is_deleted
)
select
	workplace_address,
	user_id,
	workplace_address_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where workplace_address is not null
on conflict(user_id, address) do update set
	address = EXCLUDED.address,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;
	

insert into "ODS_USERDATA".status(
	status, 
	user_id, 
	status_gain_date,
	source, 
	created, 
	modified, 
	is_deleted
)
select
	status,
	user_id,
	status_gain_date,
	status_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where status is not null
on conflict(user_id, status) do update set
	status = EXCLUDED.status,
	status_gain_date = EXCLUDED.status_gain_date,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".rzd(
	rzd_number, 
    user_id, 
    rzd_status,
    rzd_datetime,
    source, 
    created, 
    modified, 
    is_deleted
)
select
	rzd_number,
	user_id,
	rzd_status,
	rzd_datetime,
	rzd_number_source,
	CURRENT_TIMESTAMP,
 	CURRENT_TIMESTAMP,
	False
from temp_combined_data
where rzd_number is not null
on conflict(user_id, rzd_number) do update set
	rzd_number = EXCLUDED.rzd_number,
	rzd_status = EXCLUDED.rzd_status,
	rzd_datetime = EXCLUDED.rzd_datetime,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


drop table temp_combined_data;