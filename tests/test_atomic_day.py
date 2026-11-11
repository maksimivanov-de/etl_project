"""Интеграционные проверки: ETL_INTEGRATION_TESTS=1 python -m unittest discover -s tests -v.

Создают и удаляют только отдельную базу etl_atomic_test_<uuid>.
"""
import contextlib
import io
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from uuid import uuid4

import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
from sqlalchemy.exc import DataError
from sqlalchemy.exc import IntegrityError

import main
from py_scripts.config import database_settings, database_url
from py_scripts.locking import LOCK_KEY, pipeline_lock
from py_scripts.validation import DataQualityError, validate_history


@unittest.skipUnless(os.getenv('ETL_INTEGRATION_TESTS') == '1', 'Нужен ETL_INTEGRATION_TESTS=1 и локальный PostgreSQL')
class AtomicDayTests(unittest.TestCase):
    days = [('01032021', '2021-03-01'), ('02032021', '2021-03-02')]

    @classmethod
    def setUpClass(cls):
        cls.name = 'etl_atomic_test_' + uuid4().hex
        cls.admin = psycopg2.connect(**{**database_settings(), 'dbname': 'postgres'})
        cls.admin.autocommit = True
        with cls.admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.name)))
        cls.engine = create_engine(database_url().set(database=cls.name))

    @classmethod
    def tearDownClass(cls):
        cls.engine.dispose()
        try:
            with cls.admin.cursor() as cursor:
                cursor.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.name)))
        finally:
            cls.admin.close()

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = self.root / 'data'
        self.archive = self.root / 'archive'
        self.data.mkdir()
        with self.engine.begin() as connection:
            connection.exec_driver_sql('DROP SCHEMA IF EXISTS dwh CASCADE; DROP SCHEMA IF EXISTS bank CASCADE;')
            connection.exec_driver_sql('''
                CREATE SCHEMA bank;
                CREATE TABLE bank.cards(card_num text, account text, create_dt date, update_dt date);
                CREATE TABLE bank.accounts(account text, valid_to date, client text, create_dt date, update_dt date);
                CREATE TABLE bank.clients(client_id text, last_name text, first_name text, patronymic text,
                  date_of_birth date, passport_num text, passport_valid_to date, phone text, create_dt date, update_dt date);
                INSERT INTO bank.cards VALUES ('card1','acc1','2020-01-01',NULL);
                INSERT INTO bank.accounts VALUES ('acc1','2030-01-01','client1','2020-01-01',NULL);
                INSERT INTO bank.clients VALUES ('client1','Test','Only','Synthetic','1990-01-01',
                  'FAKE-001','2030-01-01','FAKE','2020-01-01',NULL);
            ''')
            main.initialize(connection.connection.driver_connection)
            connection.exec_driver_sql("INSERT INTO dwh.stg_terminals VALUES ('OLD','ATM','OLD','OLD');")
            connection.exec_driver_sql('INSERT INTO dwh.stg_transactions(transaction_id) VALUES (-1);')
        for day, (suffix, date) in enumerate(self.days, 1):
            pd.DataFrame([dict(terminal_id='t1', terminal_type='ATM', terminal_city=str(day),
                               terminal_address='Test')]).to_excel(self.data/f'terminals_{suffix}.xlsx', index=False)
            pd.DataFrame([dict(date=pd.Timestamp(date), passport='OTHER')]).to_excel(
                self.data/f'passport_blacklist_{suffix}.xlsx', index=False)
            pd.DataFrame([dict(transaction_id=day, transaction_date=date+' 12:00:00', card_num='card1',
                               oper_type='WITHDRAW', amount=100, oper_result='SUCCESS', terminal='t1')]).to_csv(
                self.data/f'transactions_{suffix}.txt', sep=';', index=False)

    def run_pipeline(self):
        with contextlib.redirect_stdout(io.StringIO()):
            main.run_pipeline(self.engine, self.days, self.data, self.archive)

    def snapshot(self):
        with self.engine.connect() as connection:
            tables = connection.exec_driver_sql("SELECT tablename FROM pg_tables WHERE schemaname='dwh' ORDER BY 1").scalars().all()
            return {table: connection.exec_driver_sql(
                f'SELECT row_to_json(t)::text FROM dwh."{table}" t ORDER BY row_to_json(t)::text'
            ).scalars().all() for table in tables}

    def count(self, table):
        with self.engine.connect() as connection:
            return connection.exec_driver_sql(f'SELECT count(*) FROM dwh.{table}').scalar_one()

    def assert_inputs_retained(self, count=6):
        self.assertEqual(len(list(self.data.iterdir())), count)
        self.assertEqual(len(list(self.archive.glob('*.backup'))), 0)

    def test_missing_each_required_file_stops_before_day(self):
        for filename in ['terminals_01032021.xlsx', 'passport_blacklist_01032021.xlsx', 'transactions_01032021.txt']:
            with self.subTest(filename=filename):
                path = self.data / filename
                contents = path.read_bytes()
                path.unlink()
                before = self.snapshot()
                with self.assertRaises(FileNotFoundError):
                    self.run_pipeline()
                self.assertEqual(self.snapshot(), before)
                self.assert_inputs_retained(5)
                path.write_bytes(contents)

    def test_insert_failure_restores_truncated_staging(self):
        before = self.snapshot()
        with patch.object(pd.DataFrame, 'to_sql', side_effect=RuntimeError('injected insert error')):
            with self.assertRaises(RuntimeError):
                self.run_pipeline()
        self.assertEqual(self.snapshot(), before)
        self.assert_inputs_retained()

    def test_real_postgres_insert_error_rolls_back_previous_stages(self):
        path = self.data / 'transactions_01032021.txt'
        frame = pd.read_csv(path, sep=';')
        frame['transaction_id'] = 'not-a-bigint'
        frame.to_csv(path, sep=';', index=False)
        before = self.snapshot()
        with self.assertRaises(pd.errors.DatabaseError) as error:
            self.run_pipeline()
        self.assertIsInstance(error.exception.__cause__, DataError)
        self.assertEqual(self.snapshot(), before)
        self.assert_inputs_retained()

    def test_failure_after_dwh_report_or_success_log_rolls_back_whole_day(self):
        for name in ['update_terminals_hist', 'update_transactions', 'build_rep_fraud', 'log_load']:
            with self.subTest(stage=name):
                original = getattr(main, name)
                def fail_after_write(*args, **kwargs):
                    original(*args, **kwargs)
                    raise RuntimeError('injected failure after write')
                before = self.snapshot()
                with patch.object(main, name, side_effect=fail_after_write):
                    with self.assertRaises(RuntimeError):
                        self.run_pipeline()
                self.assertEqual(self.snapshot(), before)
                self.assert_inputs_retained()
        # После устранения причины тот же набор успешно обрабатывается.
        self.run_pipeline()
        self.assertEqual(self.count('dwh_fact_transactions'), 2)
        self.assertEqual(self.count('meta_load'), 2)

    def test_second_day_failure_preserves_first_day(self):
        original = main.log_load
        def fail_second(conn, date):
            original(conn, date)
            if date == '2021-03-02':
                raise RuntimeError('injected second day error')
        with patch.object(main, 'log_load', side_effect=fail_second):
            with self.assertRaises(RuntimeError):
                self.run_pipeline()
        self.assertEqual(self.count('dwh_fact_transactions'), 1)
        self.assertEqual(self.count('meta_load'), 1)
        self.assertEqual(len(list(self.archive.glob('*.backup'))), 3)
        self.assertEqual(len(list(self.data.iterdir())), 3)
        self.run_pipeline()
        self.assertEqual(self.count('dwh_fact_transactions'), 2)

    def test_success_and_repeat(self):
        self.run_pipeline()
        before = self.snapshot()
        self.assertEqual(self.count('meta_load'), 2)
        self.assertEqual(self.count('dwh_fact_transactions'), 2)
        self.assertEqual(len(list(self.archive.glob('*.backup'))), 6)
        self.assertEqual(len(list(self.data.iterdir())), 0)
        self.run_pipeline()
        self.assertEqual(self.snapshot(), before)

    def test_archive_failure_keeps_committed_day_and_stops(self):
        with patch.object(main, 'archive_file', side_effect=OSError('injected archive error')):
            with self.assertRaisesRegex(main.ArchiveError, 'сохранён в БД'):
                self.run_pipeline()
        self.assertEqual(self.count('meta_load'), 1)
        self.assertEqual(self.count('dwh_fact_transactions'), 1)
        self.assert_inputs_retained()

    def test_main_reports_failure(self):
        with patch.object(main, 'run_pipeline', side_effect=RuntimeError('injected error')):
            with contextlib.redirect_stderr(io.StringIO()) as output:
                self.assertEqual(main.main([]), 1)
        self.assertIn('Запуск остановлен', output.getvalue())

    def first_day(self):
        with contextlib.redirect_stdout(io.StringIO()):
            main.run_pipeline(self.engine,self.days[:1],self.data,self.archive)

    def test_source_dates_close_old_versions_without_overlap(self):
        self.first_day()
        with self.engine.begin() as connection:
            connection.exec_driver_sql("UPDATE bank.cards SET account='changed',update_dt='2021-03-02'; UPDATE bank.accounts SET valid_to='2029-01-01',update_dt='2021-03-02'; UPDATE bank.clients SET phone='changed',update_dt='2021-03-02';")
            main.refresh_reference_data(connection.connection.driver_connection,'2021-03-02')
            validate_history(connection.connection.driver_connection)
        with self.engine.connect() as connection:
            for entity in ['cards','accounts','clients']:
                count=connection.exec_driver_sql(f"SELECT count(*) FROM dwh.dwh_dim_{entity}_hist WHERE '2021-03-02'::timestamp BETWEEN effective_from AND effective_to").scalar_one()
                self.assertEqual(count,1,entity)
                boundary=connection.exec_driver_sql(f"SELECT effective_to FROM dwh.dwh_dim_{entity}_hist ORDER BY effective_from LIMIT 1").scalar_one()
                self.assertEqual(str(boundary),'2021-03-01 23:59:59.999999')

    def test_delete_and_return_use_snapshot_and_source_dates(self):
        self.first_day()
        with self.engine.begin() as connection:
            connection.exec_driver_sql('DELETE FROM bank.cards')
            main.refresh_reference_data(connection.connection.driver_connection,'2021-03-02')
        with self.engine.begin() as connection:
            connection.exec_driver_sql("INSERT INTO bank.cards VALUES ('card1','acc1','2020-01-01','2021-03-03')")
            main.refresh_reference_data(connection.connection.driver_connection,'2021-03-03')
            validate_history(connection.connection.driver_connection)
        with self.engine.connect() as connection:
            rows=connection.exec_driver_sql('SELECT effective_from,deleted_flg FROM dwh.dwh_dim_cards_hist ORDER BY effective_from').all()
            self.assertEqual([(str(date),flag) for date,flag in rows],[('2020-01-01 00:00:00',0),('2021-03-02 00:00:00',1),('2021-03-03 00:00:00',0)])

    def test_retroactive_source_change_is_rejected_without_rewriting_history(self):
        self.first_day()
        with self.engine.begin() as connection:
            connection.exec_driver_sql("UPDATE bank.clients SET phone='changed',update_dt='2020-12-31'")
        before=self.snapshot()
        with self.assertRaises(DataQualityError):
            self.run_pipeline()
        self.assertEqual(self.snapshot(),before)

    def test_equal_timestamps_use_numeric_transaction_order(self):
        rows=[]
        for identifier,amount,result in [(11,100,'SUCCESS'),(1,400,'REJECT'),(10,200,'REJECT'),(2,300,'REJECT')]:
            rows.append(dict(transaction_id=identifier,transaction_date='2021-03-01 12:00:00',card_num='card1',oper_type='WITHDRAW',amount=amount,oper_result=result,terminal='t1'))
        pd.DataFrame(rows).to_csv(self.data/'transactions_01032021.txt',sep=';',index=False)
        self.first_day()
        with self.engine.connect() as connection:
            self.assertEqual(connection.exec_driver_sql("SELECT count(*) FROM dwh.rep_fraud WHERE event_type='Подбор суммы'").scalar_one(),1)

    def test_identical_transaction_duplicates_are_collapsed(self):
        path = self.data / 'transactions_01032021.txt'
        frame = pd.read_csv(path, sep=';')
        pd.concat([frame,frame]).to_csv(path,sep=';',index=False)
        self.run_pipeline()
        self.assertEqual(self.count('dwh_fact_transactions'), 2)

    def test_empty_full_snapshot_deletes_all_active_terminals(self):
        self.first_day()
        path=self.data/'terminals_02032021.xlsx'
        pd.read_excel(path).iloc[:0].to_excel(path,index=False)
        path=self.data/'transactions_02032021.txt'
        pd.read_csv(path,sep=';').iloc[:0].to_csv(path,sep=';',index=False)
        self.run_pipeline()
        self.assertEqual(self.count('meta_load'),2)
        with self.engine.connect() as connection:
            rows=connection.exec_driver_sql("SELECT effective_from,deleted_flg FROM dwh.dwh_dim_terminals_hist ORDER BY effective_from").all()
            self.assertEqual([(str(dt),flag) for dt,flag in rows],[('2021-03-01 00:00:00',0),('2021-03-02 00:00:00',1)])
            self.assertEqual(connection.exec_driver_sql('SELECT count(*) FROM dwh.v_terminals').scalar_one(),0)
        self.assertEqual(len(list(self.archive.glob('*.backup'))),6)

    def test_full_snapshot_may_delete_more_than_ten_percent(self):
        frame=pd.DataFrame([dict(terminal_id=f't{i}',terminal_type='ATM',terminal_city='1',terminal_address='Test') for i in range(1,11)])
        frame.to_excel(self.data/'terminals_01032021.xlsx',index=False)
        self.run_pipeline()
        with self.engine.connect() as connection:
            self.assertEqual(connection.exec_driver_sql('SELECT count(*) FROM dwh.v_terminals').scalar_one(),1)
            self.assertEqual(connection.exec_driver_sql('SELECT count(*) FROM dwh.dwh_dim_terminals_hist WHERE deleted_flg=1').scalar_one(),9)
        self.assertEqual(self.count('meta_load'),2)

    def test_nullable_attributes_are_not_rejected_as_missing_keys(self):
        path=self.data/'terminals_01032021.xlsx'
        frame=pd.read_excel(path)
        frame[['terminal_city','terminal_address','terminal_type']]=None
        frame.to_excel(path,index=False)
        with self.engine.begin() as connection:
            connection.exec_driver_sql('UPDATE bank.clients SET passport_num=NULL')
        self.first_day()
        self.assertEqual(self.count('dwh_fact_transactions'),1)
        self.assertEqual(self.count('meta_load'),1)

    def test_zero_byte_workbook_still_rolls_back_day(self):
        (self.data/'terminals_01032021.xlsx').write_bytes(b'')
        before=self.snapshot()
        with self.assertRaises(ValueError): self.run_pipeline()
        self.assertEqual(self.snapshot(),before)
        self.assert_inputs_retained()

    def test_wrong_date_missing_key_and_wrong_columns_are_rejected(self):
        path=self.data/'transactions_01032021.txt'
        original=pd.read_csv(path,sep=';')
        for kind in ['date','key','columns']:
            with self.subTest(kind=kind):
                frame=original.copy()
                if kind=='date': frame['transaction_date']='2021-03-02 12:00:00'
                elif kind=='key': frame['card_num']=None
                else: frame=frame.drop(columns=['terminal'])
                frame.to_csv(path,sep=';',index=False)
                before=self.snapshot()
                with self.assertRaises(DataQualityError): self.run_pipeline()
                self.assertEqual(self.snapshot(),before)
                self.assert_inputs_retained()

    def test_conflicting_transaction_duplicates_roll_back_day(self):
        path = self.data / 'transactions_01032021.txt'
        frame = pd.read_csv(path,sep=';')
        changed = frame.copy()
        changed['amount'] = 101
        pd.concat([frame,changed]).to_csv(path,sep=';',index=False)
        before = self.snapshot()
        with self.assertRaises(DataQualityError):
            self.run_pipeline()
        self.assertEqual(self.snapshot(),before)
        self.assert_inputs_retained()

    def test_conflict_with_existing_transaction_is_rejected(self):
        with self.engine.begin() as connection:
            connection.exec_driver_sql("INSERT INTO dwh.dwh_fact_transactions(trans_id,trans_date,amt) VALUES ('1','2021-03-01',200)")
        before = self.snapshot()
        with self.assertRaises(DataQualityError):
            self.run_pipeline()
        self.assertEqual(self.snapshot(),before)
        self.assert_inputs_retained()

    def test_database_rejects_duplicate_transaction_key(self):
        with self.assertRaises(IntegrityError):
            with self.engine.begin() as connection:
                connection.exec_driver_sql("INSERT INTO dwh.dwh_fact_transactions(trans_id) VALUES ('1'),('1')")
        self.assertEqual(self.count('dwh_fact_transactions'),0)

    def test_concurrent_run_is_rejected_before_any_changes(self):
        before = self.snapshot()
        with pipeline_lock(self.engine):
            with patch.object(main, 'initialize') as initialize:
                with self.assertRaisesRegex(RuntimeError, 'уже выполняется'):
                    self.run_pipeline()
                initialize.assert_not_called()
        self.assertEqual(self.snapshot(), before)
        self.assert_inputs_retained()
        self.run_pipeline()
        self.assertEqual(self.count('meta_load'), 2)

    def test_lock_is_released_after_failure_for_another_session(self):
        with patch.object(main, 'process_day', side_effect=RuntimeError('failure')):
            with self.assertRaises(RuntimeError):
                self.run_pipeline()
        other = psycopg2.connect(**{**database_settings(), 'dbname': self.name})
        try:
            with other.cursor() as cursor:
                cursor.execute('SELECT pg_try_advisory_lock(%s, %s)', LOCK_KEY)
                self.assertTrue(cursor.fetchone()[0])
                cursor.execute('SELECT pg_advisory_unlock(%s, %s)', LOCK_KEY)
        finally:
            other.close()


    def test_partial_archive_is_resumed_without_reloading_first_day(self):
        original=main.archive_file
        calls=0
        def fail_second(*args,**kwargs):
            nonlocal calls
            calls+=1
            if calls==2: raise OSError('injected mid-archive failure')
            return original(*args,**kwargs)
        with patch.object(main,'archive_file',side_effect=fail_second):
            with self.assertRaises(main.ArchiveError): self.run_pipeline()
        self.assertEqual(self.count('meta_load'),1)
        self.assertEqual(len(list(self.archive.glob('*.backup'))),1)
        self.run_pipeline()
        self.assertEqual(self.count('dwh_fact_transactions'),2)
        self.assertEqual(self.count('meta_load'),2)
        self.assertEqual(len(list(self.archive.glob('*.backup'))),6)
        with self.engine.connect() as connection:
            self.assertEqual(connection.exec_driver_sql('SELECT count(*) FROM dwh.meta_files WHERE NOT archived').scalar_one(),0)

    def test_changed_file_is_not_archived_as_original(self):
        with patch.object(main,'archive_file',side_effect=OSError('failure')):
            with self.assertRaises(main.ArchiveError): self.run_pipeline()
        path=self.data/'transactions_01032021.txt'
        path.write_text('replacement')
        with self.assertRaises(main.ArchiveError): self.run_pipeline()
        self.assertEqual(path.read_text(),'replacement')
        self.assertEqual(self.count('meta_load'),1)

    def test_report_time_is_actual_calculation_time(self):
        with self.engine.begin() as connection:
            connection.exec_driver_sql("UPDATE bank.clients SET passport_valid_to='2021-02-28'")
        self.first_day()
        with self.engine.connect() as connection:
            row=connection.exec_driver_sql("SELECT event_dt::date::text, report_dt>current_timestamp-interval '5 minutes' FROM dwh.rep_fraud LIMIT 1").one()
            self.assertEqual(row[0],'2021-03-01')
            self.assertTrue(row[1])


    def late_file(self, identifier=3, card='card1', when='2021-03-01 23:50:00'):
        path=self.data/'transactions_01032021.txt'
        pd.DataFrame([dict(transaction_id=identifier,transaction_date=when,card_num=card,
            oper_type='WITHDRAW',amount=100,oper_result='SUCCESS',terminal='t1')]).to_csv(path,sep=';',index=False)
        return path

    def test_late_transactions_rebuild_next_day_window_without_changing_history(self):
        path=self.data/'transactions_02032021.txt'
        frame=pd.read_csv(path,sep=';')
        frame['transaction_date']='2021-03-02 00:10:00'
        frame.to_csv(path,sep=';',index=False)
        self.run_pipeline()
        before={k:v for k,v in self.snapshot().items() if k.endswith('_hist')}
        original_archive=(self.archive/'transactions_01032021.txt.backup').read_bytes()
        self.late_file()
        main.run_transactions_only(self.engine,'2021-03-01',self.data,self.archive)
        self.assertEqual(self.count('dwh_fact_transactions'),3)
        self.assertEqual(self.count('meta_load'),2)
        self.assertEqual({k:v for k,v in self.snapshot().items() if k.endswith('_hist')},before)
        self.assertEqual((self.archive/'transactions_01032021.txt.backup').read_bytes(),original_archive)
        with self.engine.connect() as connection:
            self.assertEqual(connection.exec_driver_sql("SELECT count(*) FROM dwh.rep_fraud WHERE event_type='Операции в разных городах' AND event_dt::date='2021-03-02'").scalar_one(),1)
        before=self.snapshot()
        self.late_file()
        main.run_transactions_only(self.engine,'2021-03-01',self.data,self.archive)
        self.assertEqual(self.snapshot(),before)

    def test_late_transaction_without_reference_history_is_rejected(self):
        self.run_pipeline()
        path=self.late_file(card='unknown')
        before=self.snapshot()
        with self.assertRaises(DataQualityError):
            main.run_transactions_only(self.engine,'2021-03-01',self.data,self.archive)
        self.assertEqual(self.snapshot(),before)
        self.assertTrue(path.exists())

    def test_missing_older_full_day_is_not_silently_skipped(self):
        self.run_pipeline()
        with self.engine.begin() as connection:
            connection.exec_driver_sql("DELETE FROM dwh.meta_load WHERE report_dt='2021-03-01'")
        with self.assertRaisesRegex(ValueError,'transactions-only'):
            self.run_pipeline()

    def test_late_mode_does_not_mark_missing_full_day_successful(self):
        self.run_pipeline()
        with self.engine.begin() as connection:
            connection.exec_driver_sql("DELETE FROM dwh.meta_load WHERE report_dt='2021-03-01'")
        self.late_file()
        main.run_transactions_only(self.engine,'2021-03-01',self.data,self.archive)
        self.assertEqual(self.count('dwh_fact_transactions'),3)
        self.assertEqual(self.count('meta_load'),1)


    def test_expiration_date_itself_is_not_fraud(self):
        with self.engine.begin() as connection:
            connection.exec_driver_sql("UPDATE bank.clients SET passport_valid_to='2021-03-01'; UPDATE bank.accounts SET valid_to='2021-03-01'")
        self.first_day()
        self.assertEqual(self.count('rep_fraud'),0)

    def test_blacklist_date_in_future_does_not_flag_old_operation(self):
        pd.DataFrame([dict(date=pd.Timestamp('2021-03-02'),passport='FAKE-001')]).to_excel(self.data/'passport_blacklist_01032021.xlsx',index=False)
        self.first_day()
        self.assertEqual(self.count('rep_fraud'),0)

    def test_expired_contract_is_flagged(self):
        with self.engine.begin() as connection:
            connection.exec_driver_sql("UPDATE bank.accounts SET valid_to='2021-02-28'")
        self.first_day()
        with self.engine.connect() as connection:
            self.assertEqual(connection.exec_driver_sql("SELECT count(*) FROM dwh.rep_fraud WHERE event_type='Недействующий договор'").scalar_one(),1)

    def test_blacklist_effective_today_is_flagged(self):
        pd.DataFrame([dict(date=pd.Timestamp('2021-03-01'),passport='FAKE-001')]).to_excel(self.data/'passport_blacklist_01032021.xlsx',index=False)
        self.first_day()
        with self.engine.connect() as connection:
            self.assertEqual(connection.exec_driver_sql("SELECT count(*) FROM dwh.rep_fraud WHERE event_type='Заблокированный паспорт'").scalar_one(),1)

    def test_non_decreasing_amounts_do_not_match_selection_rule(self):
        rows=[]
        for identifier,amount,result in [(1,400,'REJECT'),(2,300,'REJECT'),(3,350,'REJECT'),(4,100,'SUCCESS')]:
            rows.append(dict(transaction_id=identifier,transaction_date=f'2021-03-01 12:0{identifier}:00',card_num='card1',oper_type='WITHDRAW',amount=amount,oper_result=result,terminal='t1'))
        pd.DataFrame(rows).to_csv(self.data/'transactions_01032021.txt',sep=';',index=False)
        self.first_day()
        self.assertEqual(self.count('rep_fraud'),0)


    def city_pair(self,second_time):
        path=self.data/'terminals_01032021.xlsx'
        pd.DataFrame([
            dict(terminal_id='t1',terminal_type='ATM',terminal_city='A',terminal_address='Test'),
            dict(terminal_id='t2',terminal_type='ATM',terminal_city='B',terminal_address='Test'),
        ]).to_excel(path,index=False)
        rows=[]
        for identifier,when,terminal in [(1,'10:00:00','t1'),(2,second_time,'t2')]:
            rows.append(dict(transaction_id=identifier,transaction_date='2021-03-01 '+when,
                card_num='card1',oper_type='WITHDRAW',amount=100,oper_result='SUCCESS',terminal=terminal))
        pd.DataFrame(rows).to_csv(self.data/'transactions_01032021.txt',sep=';',index=False)

    def test_cities_exactly_one_hour_apart_are_flagged(self):
        self.city_pair('11:00:00')
        self.first_day()
        with self.engine.connect() as connection:
            result=connection.exec_driver_sql("SELECT event_dt::time::text FROM dwh.rep_fraud WHERE event_type='Операции в разных городах'").scalars().all()
            self.assertEqual(result,['11:00:00'])

    def test_cities_more_than_one_hour_apart_are_not_flagged(self):
        self.city_pair('11:00:01')
        self.first_day()
        self.assertEqual(self.count('rep_fraud'),0)


if __name__ == '__main__':
    unittest.main()
