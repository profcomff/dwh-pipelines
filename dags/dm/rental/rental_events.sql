DELETE FROM "DM_RENTAL".dm_rentals_events;
INSERT INTO "DM_RENTAL".dm_rentals_events
SELECT
    rental_session.uuid as uuid,
    rental_session.user_id as user_id,
    rental_session.item_id as item_id,
    rental_session.admin_open_id as admin_session_open_id,
    rental_session.admin_close_id as admin_close_session_id,
    rental_session.reservation_ts as reservation_ts,
    rental_session.start_ts as start_ts,
    rental_session.end_ts as end_ts,
    rental_session.actual_return_ts as actual_return_ts,
    rental_session.status as status,
    item.type_id as type_id,
    item_type.name as name,
    item_type.image_url as image_url,
    item_type.description as description,
    event.session_id as session_id,
    event.admin_id as admin_strike_id,
    strike.reason as strike_reason,
    strike.create_ts as strike_date
FROM "DWH_RENTAL".rental_session as rental_session
    LEFT JOIN "DWH_RENTAL".item as item on rental_session.item_id = item.api_id
    LEFT JOIN "DWH_RENTAL".item_type as item_type on item_type.api_id = item.type_id
    LEFT JOIN "DWH_RENTAL".event as event on  event.user_id = rental_session.user_id
    LEFT JOIN "DWH_RENTAL".strike as strike on strike.user_id = rental_session.user_id
