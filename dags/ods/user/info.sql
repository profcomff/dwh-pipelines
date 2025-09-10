with source_data as(
	select id, name from "STG_USERDATA".source
),
stg_userdata_data as(
	select -- полная таблица
	  owner_id as user_id,
	  string_agg(distinct case when p.name = 'Электронная почта' then value end, ', ') as email,
	  string_agg(distinct case when p.name = 'Электронная почта' then source_data.name end, ', ') as email_source,
	  string_agg(distinct case when p.name = 'Номер телефона' then value end, ', ') as phone_number,
	  string_agg(distinct case when p.name = 'Номер телефона' then source_data.name end, ', ') as phone_number_source,
	  string_agg(distinct case when p.name = 'Имя пользователя VK' then value end, ', ') as vk_username,
	  string_agg(distinct case when p.name = 'Имя пользователя VK' then source_data.name end, ', ') as vk_username_source,
	  string_agg(distinct case when p.name = 'Город' then value end, ', ') as city,
	  string_agg(distinct case when p.name = 'Город' then source_data.name end, ', ') as city_source,
	  string_agg(distinct case when p.name = 'Родной город' then value end, ', ') as birth_city,
	  string_agg(distinct case when p.name = 'Родной город' then source_data.name end, ', ') as birth_city_source,
	  string_agg(distinct case when p.name = 'Место жительства' then value end, ', ') as address,
	  string_agg(distinct case when p.name = 'Место жительства' then source_data.name end, ', ') as address_source,
	  string_agg(distinct case when p.name = 'Имя пользователя GitHub' then value end, ', ') as git_hub_username,
	  string_agg(distinct case when p.name = 'Имя пользователя GitHub' then source_data.name end, ', ') as git_hub_username_source,
	  string_agg(distinct case when p.name = 'Имя пользователя Telegram' then value end, ', ') as telegram_username,
	  string_agg(distinct case when p.name = 'Имя пользователя Telegram' then source_data.name end, ', ') as telegram_username_source,
	  string_agg(distinct case when p.name = 'Домашний номер телефона' then value end, ', ') as home_phone_number,
	  string_agg(distinct case when p.name = 'Домашний номер телефона' then source_data.name end, ', ') as home_phone_number_source,
	  string_agg(distinct case when p.name = 'Ступень обучения' then value end, ', ') as education_level,
	  string_agg(distinct case when p.name = 'Ступень обучения' then source_data.name end, ', ') as education_level_source,
	  string_agg(distinct case when p.name = 'ВУЗ' then value end, ', ') as university,
	  string_agg(distinct case when p.name = 'ВУЗ' then source_data.name end, ', ') as university_source,
	  string_agg(distinct case when p.name = 'Факультет' then value end, ', ') as faculty,
	  string_agg(distinct case when p.name = 'Факультет' then source_data.name end, ', ') as faculty_source,
	  string_agg(distinct case when p.name = 'Академическая группа' then value end, ', ') as academic_group,
	  string_agg(distinct case when p.name = 'Академическая группа' then source_data.name end, ', ') as academic_group_source,
	  string_agg(distinct case when p.name = 'Должность' then value end, ', ') as position,
	  string_agg(distinct case when p.name = 'Должность' then source_data.name end, ', ') as position_source,
	  string_agg(distinct case when p.name = 'Номер студенческого билета' then value end, ', ') as student_id,
	  string_agg(distinct case when p.name = 'Номер студенческого билета' then source_data.name end, ', ') as student_id_source,
	  string_agg(distinct case when p.name = 'Кафедра' then value end, ', ') as department,
	  string_agg(distinct case when p.name = 'Кафедра' then source_data.name end, ', ') as department_source,
	  string_agg(distinct case when p.name = 'Форма обучения' then value end, ', ') as education_form,
	  string_agg(distinct case when p.name = 'Форма обучения' then source_data.name end, ', ') as education_form_source,
	  string_agg(distinct case when p.name = 'Полное имя' then value end, ', ') as full_name,
	  string_agg(distinct case when p.name = 'Полное имя' then source_data.name end, ', ') as full_name_source,
	  string_agg(distinct case when p.name = 'Дата рождения' then value end, ', ') as birthday,
	  string_agg(distinct case when p.name = 'Дата рождения' then source_data.name end, ', ') as birthday_source,
	  string_agg(distinct case when p.name = 'Фото' then value end, ', ') as photo,
	  string_agg(distinct case when p.name = 'Фото' then source_data.name end, ', ') as photo_source,
	  string_agg(distinct case when p.name = 'Пол' then value end, ', ') as sex,
	  string_agg(distinct case when p.name = 'Пол' then source_data.name end, ', ') as sex_source,
	  string_agg(distinct case when p.name = 'Место работы' then value end, ', ') as workplace,
	  string_agg(distinct case when p.name = 'Место работы' then source_data.name end, ', ') as workplace_source,
	  string_agg(distinct case when p.name = 'Расположение работы' then value end, ', ') as workplace_address,
	  string_agg(distinct case when p.name = 'Расположение работы' then source_data.name end, ', ') as workplace_address_source
	from "STG_USERDATA".info i
	left join "STG_USERDATA".param p on i.param_id = p.id
	join source_data on i.source_id = source_data.id
	group by owner_id
),
stg_union_member_data as(
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
		CONCAT(first_name, ' ', middle_name, ' ', last_name) as full_name,
		'union_member' as source
	from "STG_UNION_MEMBER".union_member 		
),
combined_data as(
	select
		ud.user_id as user_id,
		ud.academic_group as academic_group,
		ud.academic_group_source as academic_group_source,
		ud.address as address,
		ud.address_source as source,
		ud.birth_city as birth_city,
		ud.birth_city_source as birth_city_source,
		coalesce(ud.birthday, um.birthday) as birthday,
	    CASE 
	        WHEN um.birthday IS NOT NULL THEN um.source
	        WHEN ud.birthday IS NOT NULL THEN ud.birthday_source
	    END AS birthday_source,
	    ud.city as city,
	    ud.city_source as city_source,
	    ud.department as department,
	    ud.department_source as department_source,
	    case
	    	when um.education_form is not null and ud.education_form is not null then
	    	case
	    		when um.education_form = ud.education_form then um.education_form
	    		else CONCAT(um.education_form, ', ', ud.education_form)
	    	end
	    	when um.education_form is not null then um.education_form
	    	when ud.education_form is not null then ud.education_form
	    end  as education_form,
	    case
	    	when um.education_form is not null and ud.education_form is not null then CONCAT(um.source, ', ', ud.education_form_source)
	    	when um.education_form is not null then um.source
	    	when ud.education_form is not null then ud.education_form_source
	    end  as education_form_source,
	    case
	    	when um.education_level is not null and ud.education_level is not null then
	    	case
	    		when um.education_level = ud.education_level then um.education_level
	    		else CONCAT(um.education_level, ', ', ud.education_level)
	    	end
	    	when um.education_level is not null then um.education_level
	    	when ud.education_level is not null then ud.education_level
	    end  as education_level,
	    case
	    	when um.education_level is not null and ud.education_level is not null then CONCAT(um.source, ', ', ud.education_level_source)
	    	when um.education_level is not null then um.source
	    	when ud.education_level is not null then ud.education_level_source
	    end  as education_level_source,
	    case
	    	when um.email is not null and ud.email is not null then
	    	case
	    		when um.email = ud.email then um.email
	    		else CONCAT(um.email, ', ', ud.email)
	    	end
	    	when um.email is not null then um.email
	    	when ud.email is not null then ud.email
	    end  as email,
	    case
	    	when um.email is not null and ud.email is not null then CONCAT(um.source, ', ', ud.email_source)
	    	when um.email is not null then um.source
	    	when ud.email is not null then ud.email_source
	    end  as email_source,
	    case
	    	when um.faculty is not null and ud.faculty is not null then
	    	case
	    		when um.faculty = ud.faculty then um.faculty
	    		else CONCAT(um.faculty, ', ', ud.faculty)
	    	end
	    	when um.faculty is not null then um.faculty
	    	when ud.faculty is not null then ud.faculty
	    end  as faculty,
	    case
	    	when um.faculty is not null and ud.faculty is not null then CONCAT(um.source, ', ', ud.faculty_source)
	    	when um.faculty is not null then um.source
	    	when ud.faculty is not null then ud.faculty_source
	    end  as faculty_source,
	    case
	    	when um.full_name is not null and ud.full_name is not null then
	    	case
	    		when um.full_name = ud.full_name then um.full_name
	    		else CONCAT(um.full_name, ', ', ud.full_name)
	    	end
	    	when um.full_name is not null then um.full_name
	    	when ud.full_name is not null then ud.full_name
	    end  as full_name,
	    case
	    	when um.full_name is not null and ud.full_name is not null then CONCAT(um.source, ', ', ud.full_name_source)
	    	when um.full_name is not null then um.source
	    	when ud.full_name is not null then ud.full_name_source
	    end  as full_name_source,
	    ud.git_hub_username as git_hub_username,
	    ud.git_hub_username_source as git_hub_username_source,
	    ud.home_phone_number as home_phone_number,
	    ud.home_phone_number_source as home_phone_number_source,
	    case
	    	when um.phone_number is not null and ud.phone_number is not null then
	    	case
	    		when um.phone_number = ud.phone_number then um.phone_number
	    		else CONCAT(um.phone_number, ', ', ud.phone_number)
	    	end
	    	when um.phone_number is not null then um.phone_number
	    	when ud.phone_number is not null then ud.phone_number
	    end  as phone_number,
	    case
	    	when um.phone_number is not null and ud.phone_number is not null then CONCAT(um.source, ', ', ud.phone_number_source)
	    	when um.phone_number is not null then um.source
	    	when ud.phone_number is not null then ud.phone_number_source
	    end  as phone_number_source,
	    case
	    	when um.photo is not null and ud.photo is not null then
	    	case
	    		when um.photo = ud.photo then um.photo
	    		else CONCAT(um.photo, ', ', ud.photo)
	    	end
	    	when um.photo is not null then um.photo
	    	when ud.photo is not null then ud.photo
	    end  as photo,
	    case
	    	when um.photo is not null and ud.photo is not null then CONCAT(um.source, ', ', ud.photo_source)
	    	when um.photo is not null then um.source
	    	when ud.photo is not null then ud.photo_source
	    end  as photo_source,
	    ud.position as position,
	    ud.position_source as position_source,
	    ud.sex as sex,
	    ud.sex_source as sex_source,
	    case
	    	when um.student_id is not null and ud.student_id is not null then
	    	case
	    		when um.student_id = ud.student_id then um.student_id
	    		else CONCAT(um.student_id, ', ', ud.student_id)
	    	end
	    	when um.student_id is not null then um.student_id
	    	when ud.student_id is not null then ud.student_id
	    end  as student_id,
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
	    
	    
	from stg_userdata_data ud join stg_union_member_data um on ud.student_id = um.student_id
)
-- Написать upsertы 
-- insert into "ODS_USERDATA".academic_group (
--     "group", 
--     user_id, 
--     source, 
--     created, 
--     modified, 
--     is_deleted
-- )
-- select
--     academic_group,
--     user_id,
--     academic_group_source,
--     CURRENT_TIMESTAMP,
--     CURRENT_TIMESTAMP,
--     False
-- from stg_userdata_data
-- where academic_group is not null
-- on conflict(user_id, "group") do update set
-- 	"group" = EXCLUDED."group",
-- 	source = EXCLUDED.source,
-- 	modified = CURRENT_TIMESTAMP
--insert into "ODS_USERDATA".address (
--    address, 
--    user_id, 
--    source, 
--    created, 
--    modified, 
--    is_deleted
--)
--select
--    address,
--    user_id,
--    address_source,
--    CURRENT_TIMESTAMP,
--    CURRENT_TIMESTAMP,
--    False
--from stg_userdata_data
--where address is not null
--on conflict(user_id) do update set
--	address = EXCLUDED.address,
--	source = EXCLUDED.address_source,
--	modified = CURRENT_TIMESTAMP;