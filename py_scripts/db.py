import psycopg2

def update_terminals_hist(conn, report_date):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                update dwh.dwh_dim_terminals_hist
                set effective_to = %(dt)s::timestamp - interval '1 microsecond'
                where terminal_id in (select terminal_id from dwh.stg_new_terminals)
                and effective_to = '2999-12-31 23:59:59'
                and deleted_flg = 1
            ''', {'dt': report_date})
            cursor.execute('''
                insert into dwh.dwh_dim_terminals_hist(
                    terminal_id, terminal_type, 
                    terminal_city, terminal_address, effective_from)
                select 
                    terminal_id, terminal_type, 
                    terminal_city, terminal_address, %(dt)s::timestamp
                from dwh.stg_new_terminals
            ''', {'dt': report_date})
            cursor.execute('''
                update dwh.dwh_dim_terminals_hist
                set effective_to = %(dt)s::timestamp - interval '1 microsecond'
                where terminal_id in (select terminal_id from dwh.stg_updated_terminals)
                and effective_to = '2999-12-31 23:59:59'
            ''', {'dt': report_date})
            cursor.execute('''
                insert into dwh.dwh_dim_terminals_hist(
                    terminal_id, terminal_type, 
                    terminal_city, terminal_address, effective_from)
                select 
                    terminal_id, terminal_type, 
                    terminal_city, terminal_address, %(dt)s::timestamp
                from dwh.stg_updated_terminals
            ''', {'dt': report_date})
            cursor.execute('''
                update dwh.dwh_dim_terminals_hist
                set effective_to = %(dt)s::timestamp - interval '1 microsecond'
                where terminal_id in (select terminal_id from dwh.stg_del_terminals)
                and effective_to = '2999-12-31 23:59:59'
            ''', {'dt': report_date})
            cursor.execute('''
                insert into dwh.dwh_dim_terminals_hist(
                    terminal_id, terminal_type, 
                    terminal_city, terminal_address, deleted_flg, effective_from)
                select 
                    terminal_id, terminal_type, 
                    terminal_city, terminal_address, 1, %(dt)s::timestamp
                from dwh.stg_del_terminals
            ''', {'dt': report_date})
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def update_passport_blacklist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                insert into dwh.dwh_fact_passport_blacklist(
                    passport_num, entry_dt)
                select
                    passport as passport_num,
                    date as enrty_dt
                from dwh.stg_new_passports
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def update_transactions(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                insert into dwh.dwh_fact_transactions(
                    trans_id, trans_date,
                    card_num, oper_type, amt,
                    oper_result, terminal)
                select
                    transaction_id as trans_id,
                    transaction_date as trans_date,
                    card_num, oper_type,
                    amount as amt, oper_result, terminal
                from dwh.stg_new_transactions
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def update_cards_hist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                update dwh.dwh_dim_cards_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where card_num in (select card_num from dwh.stg_new_cards)
                and effective_to = '2999-12-31 23:59:59'
                and deleted_flg = 1
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_cards_hist(
                    card_num, account_num, effective_from)
                select 
                    card_num, account_num, coalesce(update_dt, create_dt)
                from dwh.stg_new_cards
            ''')
            cursor.execute('''
                update dwh.dwh_dim_cards_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where card_num in (select card_num from dwh.stg_updated_cards)
                and effective_to = '2999-12-31 23:59:59'
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_cards_hist(
                    card_num, account_num, effective_from)
                select 
                    card_num, account_num, coalesce(update_dt, create_dt)
                from dwh.stg_updated_cards
            ''')
            cursor.execute('''
                update dwh.dwh_dim_cards_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where card_num in (select card_num from dwh.stg_del_cards)
                and effective_to = '2999-12-31 23:59:59'
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_cards_hist(
                    card_num, account_num, deleted_flg)
                select 
                    card_num, account_num, 1
                from dwh.stg_del_cards
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def update_accounts_hist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                update dwh.dwh_dim_accounts_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where account_num in (select account_num from dwh.stg_new_accounts)
                and effective_to = '2999-12-31 23:59:59'
                and deleted_flg = 1
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_accounts_hist(
                    account_num, valid_to, client, effective_from)
                select 
                    account_num, valid_to, client, coalesce(update_dt, create_dt)
                from dwh.stg_new_accounts
            ''')
            cursor.execute('''
                update dwh.dwh_dim_accounts_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where account_num in (select account_num from dwh.stg_updated_accounts)
                and effective_to = '2999-12-31 23:59:59'
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_accounts_hist(
                    account_num, valid_to, client, effective_from)
                select 
                    account_num, valid_to, client, coalesce(update_dt, create_dt)
                from dwh.stg_updated_accounts
            ''')
            cursor.execute('''
                update dwh.dwh_dim_accounts_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where account_num in (select account_num from dwh.stg_del_accounts)
                and effective_to = '2999-12-31 23:59:59'
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_accounts_hist(
                    account_num, valid_to, client, deleted_flg)
                select 
                    account_num, valid_to, client, 1
                from dwh.stg_del_accounts
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def update_clients_hist(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute('''
                update dwh.dwh_dim_clients_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where client_id in (select client_id from dwh.stg_new_clients)
                and effective_to = '2999-12-31 23:59:59'
                and deleted_flg = 1
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_clients_hist(
                    client_id, last_name, first_name, patrinymic,
                    date_of_birth, passport_num, passport_valid_to, phone,
                    effective_from)
                select
                    client_id, last_name, first_name, patrinymic,
                    date_of_birth, passport_num, passport_valid_to, phone,
                    coalesce(update_dt, create_dt)
                from dwh.stg_new_clients
            ''')
            cursor.execute('''
                update dwh.dwh_dim_clients_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where client_id in (select client_id from dwh.stg_updated_clients)
                and effective_to = '2999-12-31 23:59:59'
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_clients_hist(
                    client_id, last_name, first_name, patrinymic,
                    date_of_birth, passport_num, passport_valid_to, phone,
                    effective_from)
                select
                    client_id, last_name, first_name, patrinymic,
                    date_of_birth, passport_num, passport_valid_to, phone,
                    coalesce(update_dt, create_dt)
                from dwh.stg_updated_clients
            ''')
            cursor.execute('''
                update dwh.dwh_dim_clients_hist
                set effective_to = current_timestamp - interval '1 microsecond'
                where client_id in (select client_id from dwh.stg_del_clients)
                and effective_to = '2999-12-31 23:59:59'
            ''')
            cursor.execute('''
                insert into dwh.dwh_dim_clients_hist(
                    client_id, last_name, first_name, patrinymic,
                    date_of_birth, passport_num, passport_valid_to, phone, deleted_flg)
                select
                    client_id, last_name, first_name, patrinymic,
                    date_of_birth, passport_num, passport_valid_to, phone, 1
                from dwh.stg_del_clients
            ''')
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def build_rep_fraud(conn, report_date):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                'delete from dwh.rep_fraud where report_dt = %(dt)s',
                {'dt': report_date})
            cursor.execute('''
                insert into dwh.rep_fraud (event_dt, passport, fio, phone, event_type, report_dt)
                    select
                        t.trans_date as event_dt,
                        cl.passport_num as passport,
                        concat_ws(' ', cl.last_name, cl.first_name, cl.patrinymic) as fio,
                        cl.phone,
                        case
                            when b.passport_num is not null then 'Заблокированный паспорт'
                            else 'Просроченный паспорт'
                        end as event_type,
                        %(dt)s::date as report_dt
                    from dwh.dwh_fact_transactions t
                    join dwh.dwh_dim_cards_hist c
                        on t.card_num = c.card_num
                        and t.trans_date between c.effective_from and c.effective_to
                        and c.deleted_flg = 0
                    join dwh.dwh_dim_accounts_hist a
                        on c.account_num = a.account_num
                        and t.trans_date between a.effective_from and a.effective_to
                        and a.deleted_flg = 0
                    join dwh.dwh_dim_clients_hist cl
                        on a.client = cl.client_id
                        and t.trans_date between cl.effective_from and cl.effective_to
                        and cl.deleted_flg = 0
                    left join dwh.dwh_fact_passport_blacklist b
                        on cl.passport_num = b.passport_num
                        and b.entry_dt <= t.trans_date
                    where t.trans_date::date = %(dt)s
                    and (b.passport_num is not null
                         or cl.passport_valid_to < t.trans_date::date)

                    union all

                    select
                        t.trans_date as event_dt,
                        cl.passport_num as passport,
                        concat_ws(' ', cl.last_name, cl.first_name, cl.patrinymic) as fio,
                        cl.phone,
                        'Недействующий договор' as event_type,
                        %(dt)s::date as report_dt
                    from dwh.dwh_fact_transactions t
                    join dwh.dwh_dim_cards_hist c
                        on t.card_num = c.card_num
                        and t.trans_date between c.effective_from and c.effective_to
                        and c.deleted_flg = 0
                    join dwh.dwh_dim_accounts_hist a
                        on c.account_num = a.account_num
                        and t.trans_date between a.effective_from and a.effective_to
                        and a.deleted_flg = 0
                    join dwh.dwh_dim_clients_hist cl
                        on a.client = cl.client_id
                        and t.trans_date between cl.effective_from and cl.effective_to
                        and cl.deleted_flg = 0
                    where t.trans_date::date = %(dt)s
                    and a.valid_to < t.trans_date::date

                    union all

                    select
                        x.trans_date as event_dt,
                        x.passport_num as passport,
                        x.fio,
                        x.phone,
                        'Операции в разных городах' as event_type,
                        %(dt)s::date as report_dt
                    from (
                        select
                            t.trans_date,
                            cl.passport_num,
                            concat_ws(' ', cl.last_name, cl.first_name, cl.patrinymic) as fio,
                            cl.phone,
                            tm.terminal_city,
                            lag(tm.terminal_city) over w as prev_city,
                            lag(t.trans_date) over w as prev_dt
                        from dwh.dwh_fact_transactions t
                        join dwh.dwh_dim_cards_hist c
                            on t.card_num = c.card_num
                            and t.trans_date between c.effective_from and c.effective_to
                            and c.deleted_flg = 0
                        join dwh.dwh_dim_accounts_hist a
                            on c.account_num = a.account_num
                            and t.trans_date between a.effective_from and a.effective_to
                            and a.deleted_flg = 0
                        join dwh.dwh_dim_clients_hist cl
                            on a.client = cl.client_id
                            and t.trans_date between cl.effective_from and cl.effective_to
                            and cl.deleted_flg = 0
                        join dwh.dwh_dim_terminals_hist tm
                            on t.terminal = tm.terminal_id
                            and t.trans_date between tm.effective_from and tm.effective_to
                            and tm.deleted_flg = 0
                        window w as (partition by a.client order by t.trans_date)
                    ) x
                    where x.trans_date::date = %(dt)s
                    and x.terminal_city <> x.prev_city
                    and x.trans_date - x.prev_dt < interval '1 hour'

                    union all

                    select
                        x.trans_date as event_dt,
                        x.passport_num as passport,
                        x.fio,
                        x.phone,
                        'Подбор суммы' as event_type,
                        %(dt)s::date as report_dt
                    from (
                        select
                            t.trans_date,
                            t.amt,
                            t.oper_result,
                            cl.passport_num,
                            concat_ws(' ', cl.last_name, cl.first_name, cl.patrinymic) as fio,
                            cl.phone,
                            lag(t.amt, 1) over w as amt1,
                            lag(t.amt, 2) over w as amt2,
                            lag(t.amt, 3) over w as amt3,
                            lag(t.oper_result, 1) over w as res1,
                            lag(t.oper_result, 2) over w as res2,
                            lag(t.oper_result, 3) over w as res3,
                            lag(t.trans_date, 3) over w as dt3
                        from dwh.dwh_fact_transactions t
                        join dwh.dwh_dim_cards_hist c
                            on t.card_num = c.card_num
                            and t.trans_date between c.effective_from and c.effective_to
                            and c.deleted_flg = 0
                        join dwh.dwh_dim_accounts_hist a
                            on c.account_num = a.account_num
                            and t.trans_date between a.effective_from and a.effective_to
                            and a.deleted_flg = 0
                        join dwh.dwh_dim_clients_hist cl
                            on a.client = cl.client_id
                            and t.trans_date between cl.effective_from and cl.effective_to
                            and cl.deleted_flg = 0
                        window w as (partition by t.card_num order by t.trans_date)
                    ) x
                    where x.trans_date::date = %(dt)s
                    and x.oper_result = 'SUCCESS'
                    and x.res1 = 'REJECT' and x.res2 = 'REJECT' and x.res3 = 'REJECT'
                    and x.amt < x.amt1 and x.amt1 < x.amt2 and x.amt2 < x.amt3
                    and x.trans_date - x.dt3 <= interval '20 minutes'
            ''', {'dt': report_date})
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False


def get_last_load_date(conn):
    with conn.cursor() as cursor:
        cursor.execute('select max(report_dt) from dwh.meta_load')
        return cursor.fetchone()[0]


def log_load(conn, report_date):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                'insert into dwh.meta_load (report_dt) values (%s)',
                [report_date])
        conn.commit()
        return True
    except psycopg2.Error as e:
        conn.rollback()
        print(f'[!] Ошибка {e}')
        return False
