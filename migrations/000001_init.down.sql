BEGIN;
    DROP TABLE tab_transfers;
    DROP TABLE tab_balances;
    DROP FUNCTION internal_func_assert_isolation;
    DROP FUNCTION internal_func_balance;
    DROP FUNCTION func_balance;
    DROP FUNCTION func_transfer;
    DROP SEQUENCE seq_transfers;
COMMIT;