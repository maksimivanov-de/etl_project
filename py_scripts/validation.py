"""Проверки бизнес-данных до применения инкрементов."""


class DataQualityError(ValueError):
    pass


STAGING_COLUMNS = {
    'stg_terminals': {'terminal_id','terminal_type','terminal_city','terminal_address'},
    'stg_passport_blacklist': {'date','passport'},
    'stg_transactions': {'transaction_id','transaction_date','card_num','oper_type','amount','oper_result','terminal'},
}


def validate_columns(frame, table):
    if set(frame.columns) != STAGING_COLUMNS[table]:
        raise DataQualityError(f'{table}: состав столбцов не соответствует формату источника.')


def validate_required(conn, table, columns, key=None):
    # Имена передаются только из констант приложения.
    with conn.cursor() as cursor:
        condition = ' OR '.join(f"{column} IS NULL OR btrim({column}::text)=''" for column in columns)
        cursor.execute(f'SELECT 1 FROM dwh.{table} WHERE {condition} LIMIT 1')
        if cursor.fetchone():
            raise DataQualityError(f'{table}: отсутствуют обязательные значения.')
        if key:
            cursor.execute(f'SELECT 1 FROM dwh.{table} GROUP BY {key} HAVING count(*)>1 LIMIT 1')
            if cursor.fetchone():
                raise DataQualityError(f'{table}: повторяется ключ источника.')


def validate_file_staging(conn, report_date, transactions_only=False):
    validate_required(conn, 'stg_transactions', ['transaction_id','transaction_date','card_num','terminal'])
    if not transactions_only:
        validate_required(conn, 'stg_terminals', ['terminal_id'], 'terminal_id')
        validate_required(conn, 'stg_passport_blacklist', ['passport','date'], 'passport')
    with conn.cursor() as cursor:
        cursor.execute('SELECT 1 FROM dwh.stg_transactions WHERE transaction_date::date<>%s::date LIMIT 1', (report_date,))
        if cursor.fetchone():
            raise DataQualityError('Дата операции не соответствует дате пакета.')


def validate_reference_staging(conn):
    for table, columns, key in [
        ('stg_cards',['card_num','account'],'card_num'),
        ('stg_accounts',['account','client'],'account'),
        ('stg_clients',['client_id'],'client_id'),
    ]:
        validate_required(conn, table, columns, key)


def validate_source_history_changes(conn, entity, key, report_date):
    history=f'dwh.dwh_dim_{entity}_hist'
    with conn.cursor() as cursor:
        for kind in ['new','updated']:
            stage=f'dwh.stg_{kind}_{entity}'
            cursor.execute(f'''SELECT 1 FROM {stage}
                WHERE coalesce(update_dt,create_dt) IS NULL
                   OR coalesce(update_dt,create_dt)>%s::date LIMIT 1''',(report_date,))
            if cursor.fetchone():
                raise DataQualityError(f'{entity}: отсутствует дата изменения или она позже даты выгрузки.')
            cursor.execute(f'''SELECT 1 FROM {stage} s JOIN {history} h ON s.{key}=h.{key}
                WHERE h.effective_to='2999-12-31 23:59:59'
                  AND coalesce(s.update_dt,s.create_dt)::timestamp<=h.effective_from LIMIT 1''')
            if cursor.fetchone():
                raise DataQualityError(f'{entity}: изменение не позже текущей версии; требуется отдельный пересчёт истории.')
            cursor.execute(f'''SELECT 1 FROM {stage}
                WHERE coalesce(update_dt,create_dt)<=(SELECT max(report_dt) FROM dwh.meta_load) LIMIT 1''')
            if cursor.fetchone():
                raise DataQualityError(f'{entity}: изменение затрагивает уже обработанный день; автоматическое изменение истории задним числом запрещено.')
        cursor.execute(f'''SELECT 1 FROM dwh.stg_del_{entity} s JOIN {history} h ON s.{key}=h.{key}
            WHERE h.effective_to='2999-12-31 23:59:59' AND h.effective_from>=%s::timestamp LIMIT 1''',(report_date,))
        if cursor.fetchone():
            raise DataQualityError(f'{entity}: удаление не позже начала версии.')


def validate_history(conn):
    for entity,key in [('cards','card_num'),('accounts','account_num'),('clients','client_id'),('terminals','terminal_id')]:
        table=f'dwh.dwh_dim_{entity}_hist'
        with conn.cursor() as cursor:
            cursor.execute(f'''SELECT 1 FROM {table} WHERE effective_from IS NULL OR effective_to IS NULL
                OR effective_from>effective_to OR deleted_flg NOT IN (0,1) LIMIT 1''')
            if cursor.fetchone():
                raise DataQualityError(f'{entity}: недопустимый интервал истории.')
            cursor.execute(f'''SELECT 1 FROM {table} a JOIN {table} b ON a.{key}=b.{key} AND a.id<b.id
                WHERE a.effective_from<=b.effective_to AND b.effective_from<=a.effective_to LIMIT 1''')
            if cursor.fetchone():
                raise DataQualityError(f'{entity}: пересекаются версии истории; требуется проверка существующих данных.')


def validate_transaction_coverage(conn):
    with conn.cursor() as cursor:
        cursor.execute('''SELECT 1 FROM dwh.stg_transactions t
            WHERE (SELECT count(*) FROM dwh.dwh_dim_cards_hist c
                JOIN dwh.dwh_dim_accounts_hist a ON c.account_num=a.account_num
                    AND t.transaction_date BETWEEN a.effective_from AND a.effective_to AND a.deleted_flg=0
                JOIN dwh.dwh_dim_clients_hist cl ON a.client=cl.client_id
                    AND t.transaction_date BETWEEN cl.effective_from AND cl.effective_to AND cl.deleted_flg=0
                JOIN dwh.dwh_dim_terminals_hist tm ON t.terminal=tm.terminal_id
                    AND t.transaction_date BETWEEN tm.effective_from AND tm.effective_to AND tm.deleted_flg=0
                WHERE t.card_num=c.card_num AND t.transaction_date BETWEEN c.effective_from AND c.effective_to
                    AND c.deleted_flg=0)<>1 LIMIT 1''')
        if cursor.fetchone():
            raise DataQualityError('Для операции нет единственного полного набора исторических справочников. Требуется проверка истории; операция не будет молча пропущена.')


def validate_transactions(conn):
    with conn.cursor() as cursor:
        cursor.execute('''
            SELECT transaction_id FROM dwh.stg_transactions
            GROUP BY transaction_id
            HAVING count(DISTINCT ROW(transaction_date,card_num,oper_type,amount,oper_result,terminal)) > 1
            LIMIT 1
        ''')
        if cursor.fetchone():
            raise DataQualityError('Во входном пакете разные данные для одного ID транзакции.')
        cursor.execute('''
            SELECT 1 FROM dwh.stg_transactions s
            JOIN dwh.dwh_fact_transactions t ON s.transaction_id::varchar=t.trans_id
            WHERE ROW(s.transaction_date,s.card_num,s.oper_type,s.amount,s.oper_result,s.terminal)
              IS DISTINCT FROM ROW(t.trans_date,t.card_num,t.oper_type,t.amt,t.oper_result,t.terminal)
            LIMIT 1
        ''')
        if cursor.fetchone():
            raise DataQualityError('Транзакция с таким ID уже сохранена с другими данными.')
        cursor.execute('''
            DELETE FROM dwh.stg_transactions a USING dwh.stg_transactions b
            WHERE a.transaction_id=b.transaction_id AND a.ctid>b.ctid
        ''')
