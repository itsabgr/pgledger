BEGIN;
    DROP TABLE transfers;
    DROP TABLE balances;
    DROP FUNCTION internal_func_assert_isolation;
    DROP FUNCTION internal_func_balance;
    DROP FUNCTION func_balance;
    DROP FUNCTION func_transfer;
COMMIT;