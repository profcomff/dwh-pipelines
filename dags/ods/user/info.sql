create temporary table temp_combined_data as
with temp_source_data as(
	select id, name from "STG_USERDATA".source
),
temp_stg_userdata_data as(
	select
	  owner_id::varchar as user_id,
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
	  array_agg(distinct (case when p.name = 'Полное имя' then value end) order by (case when p.name = 'Полное имя' then value end)) as full_names,
	  array_to_string(array_agg(distinct (case when p.name = 'Полное имя' then value end) order by (case when p.name = 'Полное имя' then value end)), ', ') as full_name,
	  array_agg(distinct (case when p.name = 'Полное имя' then temp_source_data.name end) order by (case when p.name = 'Полное имя' then temp_source_data.name end)) as full_name_sources,
	  array_to_string(array_agg(distinct (case when p.name = 'Полное имя' then temp_source_data.name end) order by (case when p.name = 'Полное имя' then temp_source_data.name end)), ', ') as full_name_source,
	  string_agg(distinct case when p.name = 'Дата рождения' then value end, ', ') as birthday,
	  string_agg(distinct case when p.name = 'Дата рождения' then temp_source_data.name end, ', ') as birthday_source,
	  string_agg(distinct case when p.name = 'Фото' then value end, ', ') as photo,
	  string_agg(distinct case when p.name = 'Фото' then temp_source_data.name end, ', ') as photo_source,
	  string_agg(distinct case when p.name = 'Пол' then value end, ', ') as sex,
	  string_agg(distinct case when p.name = 'Пол' then temp_source_data.name end, ', ') as sex_source,
	  string_agg(distinct case when p.name = 'Место работы' then value end, ', ') as workplace,
	  string_agg(distinct case when p.name = 'Место работы' then temp_source_data.name end, ', ') as workplace_source,
	  string_agg(distinct case when p.name = 'Расположение работы' then value end, ', ') as workplace_address,
	  string_agg(distinct case when p.name = 'Расположение работы' then temp_source_data.name end, ', ') as workplace_address_source,
	  array_agg(distinct
	  (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[1]
	  end) order by (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[1]
	  end)) as first_names_if,  -- из формата Имя Фамилия

	  array_agg(distinct
	  (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[2]
	  end) order by (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[2]
	  end)) as last_names_if,   -- из формата Имя Фамилия

	  array_agg(distinct
	  (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[2]
	  end) order by (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[2]
	  end)) as first_names_fio, -- из формата Фамилия Имя Отчество (ВТОРОЙ элемент - имя)

	  array_agg(distinct
	  (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[1]
	  end) order by (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) >= 2 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[1]
	  end)) as last_names_fio,  -- из формата Фамилия Имя Отчество (ПЕРВЫЙ элемент - фамилия)

	  array_agg(distinct
	  (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) = 3 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[1]
	  end) order by (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) = 3 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[1]
	  end)) as first_names_otf, -- из формата Имя Отчество Фамилия

	  array_agg(distinct
	  (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) = 3 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[3]
	  end) order by (case when p.name = 'Полное имя' and array_length(string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '), 1) = 3 then
		  (string_to_array(trim(regexp_replace(lower(value), '\s+', ' ', 'g')), ' '))[3]
	  end)) as last_names_otf  -- из формата Имя Отчество Фамилия
	from "STG_USERDATA".info i
	left join "STG_USERDATA".param p on i.param_id = p.id
	join temp_source_data on i.source_id = temp_source_data.id
	group by owner_id
),
temp_stg_union_member_data as(
	select
		id::varchar as user_id,
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
		card_id::varchar as card_id,
		card_status::varchar as card_status,
		card_date::varchar as card_date,
		card_number::varchar as card_number,
		card_user::varchar as card_user,
		student_id as student_id,
		first_name as first_name,
		last_name as last_name,
		CONCAT_WS(' ',first_name, last_name) as full_name, --TODO добавить middle_name
		'union_member' as source
	from "STG_UNION_MEMBER".union_member
),
temp_union_data as (
select
		ud.user_id as user_id,
		ud.academic_group as academic_group,
		ud.academic_group_source as academic_group_source,
		ud.address as address,
		ud.address_source as address_source,
		ud.birth_city as birth_city,
		ud.birth_city_source as birth_city_source,
		COALESCE(
			CASE
				WHEN ud.birthday ~ '^\d{2}\.\d{2}\.\d{4}$' THEN
					TO_TIMESTAMP(ud.birthday, 'DD.MM.YYYY')
				ELSE NULL
			END,
			CASE
				WHEN um.birthday ~ '^\d{4}-\d{2}-\d{2}' THEN
					um.birthday::TIMESTAMP
				ELSE NULL
			END
		) AS birthday,
	    CASE
	        WHEN um.birthday IS NOT NULL AND um.birthday ~ '^\d{4}-\d{2}-\d{2}' THEN um.source
	        WHEN ud.birthday IS NOT NULL AND ud.birthday ~ '^\d{2}\.\d{2}\.\d{4}$' THEN ud.birthday_source
	    END AS birthday_source,
	    ud.city as city,
	    ud.city_source as city_source,
	    ud.department as department,
	    ud.department_source as department_source,
	    CASE
	    	WHEN um.education_form IS NOT NULL AND ud.education_form IS NOT NULL AND um.education_form != ud.education_form THEN CONCAT_WS(', ', um.education_form, ud.education_form)
	    	WHEN um.education_form IS NOT NULL THEN um.education_form
	    	WHEN ud.education_form IS NOT NULL THEN ud.education_form
	    END as education_form,
	    case
	    	when um.education_form is not null and ud.education_form is not null and um.education_form != ud.education_form then CONCAT(um.source, ', ', ud.education_form_source)
	    	when um.education_form is not null then um.source
	    	when ud.education_form is not null then ud.education_form_source
	    end  as education_form_source,
	   CASE
	    	WHEN um.education_level IS NOT NULL AND ud.education_level IS NOT NULL AND um.education_level != ud.education_level THEN CONCAT_WS(', ', um.education_level, ud.education_level)
	    	WHEN um.education_level IS NOT NULL THEN um.education_level
	    	WHEN ud.education_level IS NOT NULL THEN ud.education_level
	    END as education_level,
	    case
	    	when um.education_level is not null and ud.education_level is not null and um.education_level != ud.education_level then CONCAT(um.source, ', ', ud.education_level_source)
	    	when um.education_level is not null then um.source
	    	when ud.education_level is not null then ud.education_level_source
	    end  as education_level_source,
	    CASE
	    	WHEN um.email IS NOT NULL AND ud.email IS NOT NULL AND um.email != ud.email THEN CONCAT_WS(', ', um.email, ud.email)
	    	WHEN um.email IS NOT NULL THEN um.email
	    	WHEN ud.email IS NOT NULL THEN ud.email
	    END as email,
	    case
	    	when um.email is not null and ud.email is not null and um.email != ud.email then CONCAT(um.source, ', ', ud.email_source)
	    	when um.email is not null then um.source
	    	when ud.email is not null then ud.email_source
	    end  as email_source,
	    CASE
	    	WHEN um.faculty IS NOT NULL AND ud.faculty IS NOT NULL AND um.faculty != ud.faculty THEN CONCAT_WS(', ', um.faculty, ud.faculty)
	    	WHEN um.faculty IS NOT NULL THEN um.faculty
	    	WHEN ud.faculty IS NOT NULL THEN ud.faculty
	    END as faculty,
	    case
	    	when um.faculty is not null and ud.faculty is not null and um.faculty != ud.faculty then CONCAT(um.source, ', ', ud.faculty_source)
	    	when um.faculty is not null then um.source
	    	when ud.faculty is not null then ud.faculty_source
	    end  as faculty_source,
	    CASE
	    	WHEN um.full_name IS NOT NULL AND ud.full_names IS NOT NULL AND um.full_name != ANY(ud.full_names) THEN array_to_string(array_append(ud.full_names, um.full_name), ', ')
	    	WHEN um.full_name IS NOT NULL THEN um.full_name
	    	WHEN ud.full_names IS NOT NULL THEN array_to_string(ud.full_names, ', ')
	    END as full_name,
	    case
	    	when um.full_name is not null and ud.full_names is not null and um.full_name != ANY(ud.full_names) then CONCAT(um.source, ', ', array_to_string(ud.full_name_sources, ', '))
	    	when um.full_name is not null then um.source
	    	when ud.full_names is not null then array_to_string(ud.full_name_sources, ', ')
	    end  as full_name_source,
	    ud.git_hub_username as git_hub_username,
	    ud.git_hub_username_source as git_hub_username_source,
	    ud.home_phone_number as home_phone_number,
	    ud.home_phone_number_source as home_phone_number_source,
	    CASE
	    	WHEN um.phone_number IS NOT NULL AND ud.phone_number IS NOT NULL AND um.phone_number != ud.phone_number THEN CONCAT_WS(', ', um.phone_number, ud.phone_number)
	    	WHEN um.phone_number IS NOT NULL THEN um.phone_number
	    	WHEN ud.phone_number IS NOT NULL THEN ud.phone_number
	    END as phone_number,
	    case
	    	when um.phone_number is not null and ud.phone_number is not null and um.phone_number != ud.phone_number then CONCAT(um.source, ', ', ud.phone_number_source)
	    	when um.phone_number is not null then um.source
	    	when ud.phone_number is not null then ud.phone_number_source
	    end  as phone_number_source,
	    CASE
	    	WHEN um.photo IS NOT NULL AND ud.photo IS NOT NULL AND um.photo != ud.photo THEN CONCAT_WS(', ', um.photo, ud.photo)
	    	WHEN um.photo IS NOT NULL THEN um.photo
	    	WHEN ud.photo IS NOT NULL THEN ud.photo
	    END as photo,
	    case
	    	when um.photo is not null and ud.photo is not null and um.photo != ud.photo then CONCAT(um.source, ', ', ud.photo_source)
	    	when um.photo is not null then um.source
	    	when ud.photo is not null then ud.photo_source
	    end  as photo_source,
	    ud.position as position,
	    ud.position_source as position_source,
	    ud.sex as sex,
	    ud.sex_source as sex_source,
	    ud.student_id as student_id,
	    COALESCE(ud.student_id_source, um.source) as student_id_source,
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
	    CASE
	    	WHEN um.status IS NOT NULL AND um.status_gain_date IS NOT NULL THEN um.status
	    END as status,
	    CASE
	    	WHEN um.status IS NOT NULL AND um.status_gain_date IS NOT NULL THEN um.source
	    END as status_source,
	    CASE
	    	WHEN um.status IS NOT NULL AND um.status_gain_date IS NOT NULL THEN um.status_gain_date
	    END as status_gain_date,
	    CASE
	    	WHEN um.rzd_number IS NOT NULL AND um.rzd_status IS NOT NULL THEN um.rzd_number
	    END as rzd_number,
	    CASE
	    	WHEN um.rzd_number IS NOT NULL AND um.rzd_status IS NOT NULL THEN um.source
	    END as rzd_number_source,
	    CASE
	    	WHEN um.rzd_number IS NOT NULL AND um.rzd_status IS NOT NULL THEN um.rzd_status
	    END as rzd_status,
	    CASE
	    	WHEN um.rzd_number IS NOT NULL AND um.rzd_status IS NOT NULL AND um.rzd_datetime IS NOT NULL THEN um.rzd_datetime
	    END as rzd_datetime,
	    um.card_id as card_id,
	    um.card_status as card_status,
	    um.card_date as card_date,
	    um.card_number as card_number,
	    um.card_user as card_user
from temp_stg_userdata_data ud left join temp_stg_union_member_data um on (
	ud.student_id is not null and trim(ud.student_id) != '' and ud.student_id = um.student_id and um.student_id is not null and trim(um.student_id) != ''
	and (
		(
			exists (select 1 from unnest(ud.first_names_if) as fn where lower(trim(fn)) = lower(trim(um.first_name)))
			and exists (select 1 from unnest(ud.last_names_if) as ln where lower(trim(ln)) = lower(trim(um.last_name)))
			and ud.first_names_if is not null and array_length(ud.first_names_if, 1) > 0
			and ud.last_names_if is not null and array_length(ud.last_names_if, 1) > 0
			and um.first_name is not null and um.last_name is not null
			and trim(um.first_name) != '' and trim(um.last_name) != ''
		)
		or (
			exists (select 1 from unnest(ud.first_names_fio) as fn where lower(trim(fn)) = lower(trim(um.first_name)))
			and exists (select 1 from unnest(ud.last_names_fio) as ln where lower(trim(ln)) = lower(trim(um.last_name)))
			and ud.first_names_fio is not null and array_length(ud.first_names_fio, 1) > 0
			and ud.last_names_fio is not null and array_length(ud.last_names_fio, 1) > 0
			and um.first_name is not null and um.last_name is not null
			and trim(um.first_name) != '' and trim(um.last_name) != ''
		)
		or (
			exists (select 1 from unnest(ud.first_names_otf) as fn where lower(trim(fn)) = lower(trim(um.first_name)))
			and exists (select 1 from unnest(ud.last_names_otf) as ln where lower(trim(ln)) = lower(trim(um.last_name)))
			and ud.first_names_otf is not null and array_length(ud.first_names_otf, 1) > 0
			and ud.last_names_otf is not null and array_length(ud.last_names_otf, 1) > 0
			and um.first_name is not null and um.last_name is not null
			and trim(um.first_name) != '' and trim(um.last_name) != ''
		)
	)
)
where um.user_id is not null
)
select
	user_id,
	academic_group,
	academic_group_source,
	address,
	address_source,
	birth_city,
	birth_city_source,
	birthday,
	birthday_source,
	city,
	city_source,
	department,
	department_source,
	education_form,
	education_form_source,
	education_level,
	education_level_source,
	email,
	email_source,
	faculty,
	faculty_source,
	full_name,
	full_name_source,
	git_hub_username,
	git_hub_username_source,
	home_phone_number,
	home_phone_number_source,
	phone_number,
	phone_number_source,
	photo,
	photo_source,
	position,
	position_source,
	sex,
	sex_source,
	student_id,
	student_id_source,
	telegram_username,
	telegram_username_source,
	university,
	university_source,
	vk_username,
	vk_username_source,
	workplace,
	workplace_source,
	workplace_address,
	workplace_address_source,
	status,
	status_source,
	status_gain_date,
	rzd_number,
	rzd_number_source,
	rzd_status,
	rzd_datetime,
	card_id,
	card_status,
	card_date,
	card_number,
	card_user
from (
	select *,
		row_number() over (
			partition by user_id
			order by
				(case when academic_group is not null and trim(academic_group) != '' then 1 else 0 end +
				 case when address is not null and trim(address) != '' then 1 else 0 end +
				 case when birth_city is not null and trim(birth_city) != '' then 1 else 0 end +
				 case when city is not null and trim(city) != '' then 1 else 0 end +
				 case when department is not null and trim(department) != '' then 1 else 0 end +
				 case when education_form is not null and trim(education_form) != '' then 1 else 0 end +
				 case when education_level is not null and trim(education_level) != '' then 1 else 0 end +
				 case when email is not null and trim(email) != '' then 1 else 0 end +
				 case when faculty is not null and trim(faculty) != '' then 1 else 0 end +
				 case when full_name is not null and trim(full_name) != '' then 1 else 0 end +
				 case when git_hub_username is not null and trim(git_hub_username) != '' then 1 else 0 end +
				 case when home_phone_number is not null and trim(home_phone_number) != '' then 1 else 0 end +
				 case when phone_number is not null and trim(phone_number) != '' then 1 else 0 end +
				 case when photo is not null and trim(photo) != '' then 1 else 0 end +
				 case when position is not null and trim(position) != '' then 1 else 0 end +
				 case when sex is not null and trim(sex) != '' then 1 else 0 end +
				 case when student_id is not null and trim(student_id) != '' then 1 else 0 end +
				 case when telegram_username is not null and trim(telegram_username) != '' then 1 else 0 end +
				 case when university is not null and trim(university) != '' then 1 else 0 end +
				 case when vk_username is not null and trim(vk_username) != '' then 1 else 0 end +
				 case when workplace is not null and trim(workplace) != '' then 1 else 0 end +
				 case when workplace_address is not null and trim(workplace_address) != '' then 1 else 0 end +
				 case when status is not null and trim(status) != '' then 1 else 0 end +
				 case when rzd_number is not null and trim(rzd_number) != '' then 1 else 0 end +
				 case when card_id is not null and trim(card_id) != '' then 1 else 0 end +
				 case when card_status is not null and trim(card_status) != '' then 1 else 0 end +
				 case when card_date is not null and trim(card_date) != '' then 1 else 0 end +
				 case when card_number is not null and trim(card_number) != '' then 1 else 0 end +
				 case when card_user is not null and trim(card_user) != '' then 1 else 0 end) desc,
				user_id
		) as rn
	from temp_union_data
) deduplicated
where rn = 1;

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
from (
	select distinct
		academic_group,
		user_id::integer as user_id,
		academic_group_source
	from temp_combined_data
	where academic_group is not null and trim(academic_group) != '' and academic_group_source is not null and trim(academic_group_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		address,
		user_id::integer as user_id,
		address_source
	from temp_combined_data
	where address is not null and trim(address) != '' and address_source is not null and trim(address_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		birth_city,
		user_id::integer as user_id,
		birth_city_source
	from temp_combined_data
	where birth_city is not null and trim(birth_city) != '' and birth_city_source is not null and trim(birth_city_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		birthday,
		user_id::integer as user_id,
		birthday_source
	from temp_combined_data
	where birthday is not null and birthday_source is not null and trim(birthday_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		city,
		user_id::integer as user_id,
		city_source
	from temp_combined_data
	where city is not null and trim(city) != '' and city_source is not null and trim(city_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		department,
		user_id::integer as user_id,
		department_source
	from temp_combined_data
	where department is not null and trim(department) != '' and department_source is not null and trim(department_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		education_form,
		user_id::integer as user_id,
		education_form_source
	from temp_combined_data
	where education_form is not null and trim(education_form) != '' and education_form_source is not null and trim(education_form_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		education_level,
		user_id::integer as user_id,
		education_level_source
	from temp_combined_data
	where education_level is not null and trim(education_level) != '' and education_level_source is not null and trim(education_level_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		email,
		user_id::integer as user_id,
		email_source
	from temp_combined_data
	where email is not null and trim(email) != '' and email_source is not null and trim(email_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		faculty,
		user_id::integer as user_id,
		faculty_source
	from temp_combined_data
	where faculty is not null and trim(faculty) != '' and faculty_source is not null and trim(faculty_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		full_name,
		user_id::integer as user_id,
		full_name_source
	from temp_combined_data
	where full_name is not null and trim(full_name) != '' and full_name_source is not null and trim(full_name_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		git_hub_username,
		user_id::integer as user_id,
		git_hub_username_source
	from temp_combined_data
	where git_hub_username is not null and trim(git_hub_username) != '' and git_hub_username_source is not null and trim(git_hub_username_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		home_phone_number,
		user_id::integer as user_id,
		home_phone_number_source
	from temp_combined_data
	where home_phone_number is not null and trim(home_phone_number) != '' and home_phone_number_source is not null and trim(home_phone_number_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		phone_number,
		user_id::integer as user_id,
		phone_number_source
	from temp_combined_data
	where phone_number is not null and trim(phone_number) != '' and phone_number_source is not null and trim(phone_number_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		photo,
		user_id::integer as user_id,
		photo_source
	from temp_combined_data
	where photo is not null and trim(photo) != '' and photo_source is not null and trim(photo_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		position,
		user_id::integer as user_id,
		position_source
	from temp_combined_data
	where position is not null and trim(position) != '' and position_source is not null and trim(position_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		sex,
		user_id::integer as user_id,
		sex_source
	from temp_combined_data
	where sex is not null and trim(sex) != '' and sex_source is not null and trim(sex_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		student_id,
		user_id::integer as user_id,
		student_id_source
	from temp_combined_data
	where student_id is not null and trim(student_id) != '' and student_id_source is not null and trim(student_id_source) != ''
) dedup
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
from (
	select distinct
		telegram_username,
		user_id::integer as user_id,
		telegram_username_source
	from temp_combined_data
	where telegram_username is not null and trim(telegram_username) != '' and telegram_username_source is not null and trim(telegram_username_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		university,
		user_id::integer as user_id,
		university_source
	from temp_combined_data
	where university is not null and trim(university) != '' and university_source is not null and trim(university_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		vk_username,
		user_id::integer as user_id,
		vk_username_source
	from temp_combined_data
	where vk_username is not null and trim(vk_username) != '' and vk_username_source is not null and trim(vk_username_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		workplace,
		user_id::integer as user_id,
		workplace_source
	from temp_combined_data
	where workplace is not null and trim(workplace) != '' and workplace_source is not null and trim(workplace_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		workplace_address,
		user_id::integer as user_id,
		workplace_address_source
	from temp_combined_data
	where workplace_address is not null and trim(workplace_address) != '' and workplace_address_source is not null and trim(workplace_address_source) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		status,
		user_id::integer as user_id,
		status_gain_date,
		status_source
	from temp_combined_data
	where status is not null and trim(status) != '' and status_source is not null and trim(status_source) != '' and status_gain_date is not null
	and student_id is not null and trim(student_id) != ''
) dedup
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
from (
	select distinct
		rzd_number,
		user_id::integer as user_id,
		rzd_status,
		rzd_datetime,
		rzd_number_source
	from temp_combined_data
	where rzd_number is not null and trim(rzd_number) != '' and rzd_number_source is not null and trim(rzd_number_source) != '' and rzd_status is not null and trim(rzd_status) != '' and rzd_datetime is not null
	and student_id is not null and trim(student_id) != ''
) dedup
on conflict(user_id, rzd_number) do update set
	rzd_number = EXCLUDED.rzd_number,
	rzd_status = EXCLUDED.rzd_status,
	rzd_datetime = EXCLUDED.rzd_datetime,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


insert into "ODS_USERDATA".card(
	card_id,
	card_status,
	card_date,
	card_number,
	card_user,
	user_id,
	source,
	created,
	modified,
	is_deleted
)
select
	card_id,
	card_status,
	card_date,
	card_number,
	card_user,
	user_id,
	'union_member' as source,
	CURRENT_TIMESTAMP,
	CURRENT_TIMESTAMP,
	False
from (
	select distinct
		card_id,
		card_status,
		card_date,
		card_number,
		card_user,
		user_id::integer as user_id
	from temp_combined_data
	where card_id is not null and trim(card_id) != '' 
		and card_status is not null and trim(card_status) != ''
		and card_date is not null and trim(card_date) != ''
		and card_number is not null and trim(card_number) != ''
		and card_user is not null and trim(card_user) != ''
	and student_id is not null and trim(student_id) != ''
) dedup
on conflict(card_id, user_id) do update set
	card_status = EXCLUDED.card_status,
	card_date = EXCLUDED.card_date,
	card_number = EXCLUDED.card_number,
	card_user = EXCLUDED.card_user,
	source = EXCLUDED.source,
	modified = CURRENT_TIMESTAMP;


drop table temp_combined_data;