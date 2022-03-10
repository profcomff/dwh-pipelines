from sqlalchemy import create_engine


# Заранее должна быть создана history_table
def supply_data(url, history_table, table, data_columns):
    _SQL_SUPPLY_ = """
            -- Перекладываем с сохранением истории
            
            INSERT INTO {history_table} (id, {data_columns}, is_deleted, create_ts)
            -- Получаем последнюю версию данных, которые есть в таблице с историей
            WITH LAST_ODS_VERSION AS (  
                SELECT *
                FROM (
                    SELECT 
                     *, 
                     row_number() over (
                         PARTITION BY id
                         ORDER BY ods.create_ts DESC
                     ) as rownum
                    FROM {history_table} ods
                ) s
                WHERE is_deleted != 1 and rownum = 1
            ),
            
            -- Ищем, какие новые записи появились
            DIFFERENCE_NEW_ITEMS AS (
                SELECT id, {data_columns} FROM {table}
                EXCEPT
                SELECT id, {data_columns} FROM LAST_ODS_VERSION  -- К перому with мы обращаемся как к таблице
            ),
            
            -- Ищем, какие записи изменились (то есть имеют прежний id но новые values) или удалились
            DIFFERENCE_DELETE_ITEMS AS (
                SELECT ods.*
                FROM LAST_ODS_VERSION ods
                LEFT JOIN {table} stg
                    ON ods.id = stg.id
                WHERE stg.id is null
            ),
            
            -- Объединяем результаты и добавляем им флаг удаления, флаг времени
            DIFFERENCE_ALL AS (
                SELECT
                    id, {data_columns},
                    0 AS is_deleted,
                    STATEMENT_TIMESTAMP() AS tech_load_ts
                FROM DIFFERENCE_NEW_ITEMS
                UNION ALL
                SELECT
                    id, {data_columns},
                    1 AS is_deleted,
                    STATEMENT_TIMESTAMP() AS tech_load_ts
                FROM DIFFERENCE_DELETE_ITEMS
            )
            
            -- Ну и в целом эти данные мы грузим в БД 
            SELECT id, {data_columns}, t.is_deleted, t.tech_load_ts
            FROM DIFFERENCE_ALL t;
            """.format(history_table=history_table, table=table, data_columns=data_columns)


    engine = create_engine(url)
    with engine.connect() as connection:
        connection.execute(_SQL_SUPPLY_)
