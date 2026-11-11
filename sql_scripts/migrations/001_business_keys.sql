ALTER TABLE dwh.dwh_fact_transactions ALTER COLUMN trans_id SET NOT NULL;
CREATE UNIQUE INDEX transactions_trans_id_uq ON dwh.dwh_fact_transactions(trans_id);
ALTER TABLE dwh.meta_load ALTER COLUMN report_dt SET NOT NULL;
CREATE UNIQUE INDEX meta_load_report_dt_uq ON dwh.meta_load(report_dt);
