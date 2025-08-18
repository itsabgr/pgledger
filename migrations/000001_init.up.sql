    
BEGIN;

	CREATE SEQUENCE seq_transfers START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1;
	
	CREATE TABLE tab_transfers (
		col_uid VARCHAR(120) UNIQUE NOT NULL CHECK (char_length(trim(col_uid)) > 0 AND char_length(trim(col_uid)) = char_length(col_uid)),
		col_seq BIGINT PRIMARY KEY DEFAULT nextval('seq_transfers') CHECK (col_seq > 0),
		col_src BIGINT NOT NULL CHECK (col_src >= 0),
		col_dst BIGINT NOT NULL CHECK (col_dst > 0),
		col_val NUMERIC NOT NULL CHECK (col_val > 0),
		CONSTRAINT constrait_different_accounts CHECK (col_src <> col_dst)
    );
 
    CREATE INDEX idx_transfers_src_seq ON tab_transfers (col_src, col_seq) INCLUDE (col_val);
	CREATE INDEX idx_transfers_dst_seq ON tab_transfers (col_dst, col_seq) INCLUDE (col_val);

  	CREATE table tab_balances (
    	col_acc BIGINT PRIMARY KEY CHECK (col_acc > 0),
    	col_seq BIGINT NOT NULL CHECK (col_seq >= 0),
    	col_val NUMERIC NOT NULL CHECK(col_val >= 0)
    );
    
	CREATE FUNCTION internal_func_assert_isolation(arg_iso_level TEXT) RETURNS VOID AS $$
	DECLARE
 		var_iso_level TEXT;
	BEGIN
		
		SELECT current_setting('transaction_isolation') INTO var_iso_level;
		ASSERT var_iso_level = arg_iso_level;

	END;
	$$ LANGUAGE plpgsql;
	

	CREATE FUNCTION func_exists(arg_uid TEXT) RETURNS BOOLEAN AS $$
	DECLARE
	BEGIN
		
		PERFORM internal_func_assert_isolation('serializable');

		ASSERT char_length(arg_uid) > 0 AND char_length(trim(arg_uid)) = char_length(arg_uid);

		RETURN EXISTS (SELECT TRUE FROM tab_transfers WHERE col_uid = arg_uid);

	END;
	$$ LANGUAGE plpgsql;
	
	
	    
	CREATE FUNCTION internal_func_balance(arg_account BIGINT) RETURNS NUMERIC AS $$
	DECLARE
 		var_sum NUMERIC = 0;
		var_balance NUMERIC = 0;
		var_max_num BIGINT = 0;
		var_cache_val NUMERIC = 0;
		var_cache_num BIGINT = 0;
	BEGIN

		ASSERT arg_account > 0;

		INSERT INTO tab_balances (col_acc,col_seq,col_val) VALUES (arg_account,0,0) ON CONFLICT DO NOTHING;

		SELECT col_val, col_seq FROM tab_balances INTO var_cache_val, var_cache_num WHERE col_acc = arg_account;
		-- RAISE NOTICE 'account cached balance % % %', arg_account, var_cache_num, var_cache_val;

		SELECT last_value FROM seq_transfers INTO var_max_num;
		ASSERT var_max_num >= var_cache_num;
		-- RAISE NOTICE 'last tab_transfers col_seq %', var_max_num;

		SELECT SUM (
			CASE
				WHEN col_src = arg_account THEN -col_val
				WHEN col_dst = arg_account THEN +col_val
				ELSE 0
			END
		) INTO var_sum FROM tab_transfers WHERE arg_account IN (col_src, col_dst) AND col_seq <= var_max_num AND col_seq > var_cache_num;

		-- RAISE NOTICE 'account sum % %', arg_account, var_sum;

		IF var_sum IS NULL THEN
			var_balance := var_cache_val;
		ELSE
			var_balance := var_cache_val + var_sum;
		END IF;

		
		-- RAISE NOTICE 'account balance % %', arg_account, var_balance;
		
		INSERT INTO tab_balances(col_acc, col_seq, col_val) VALUES (arg_account, var_max_num, var_balance)
			ON CONFLICT(col_acc) DO UPDATE SET col_seq = var_max_num, col_val = var_balance;
		-- RAISE NOTICE 'account balance cached % % %', arg_account, var_max_num, var_balance;

		RETURN var_balance;

	END;
	$$ LANGUAGE plpgsql;
	
	CREATE FUNCTION func_balance(arg_account BIGINT) RETURNS NUMERIC AS $$
	DECLARE
 		var_sum_balance NUMERIC = 0;
	BEGIN
		
		ASSERT arg_account > 0;
		
		PERFORM internal_func_assert_isolation('serializable');

		PERFORM pg_advisory_xact_lock(arg_account);

		SELECT internal_func_balance(arg_account) INTO var_sum_balance;
		ASSERT var_sum_balance >= 0;

		RETURN var_sum_balance;


	END;
	$$ LANGUAGE plpgsql;
	
	

	CREATE FUNCTION func_transfer(arg_uid TEXT, arg_sender BIGINT, arg_receiver BIGINT, arg_val numeric, arg_min numeric) RETURNS BIGINT AS $$
	DECLARE
		var_sum_balance NUMERIC = 0;
		var_new_balance NUMERIC = 0;
		var_inserted_num BIGINT = NULL;
		var_updated BOOLEAN = FALSE;
	BEGIN
	
			
			ASSERT arg_min >= 0;
			ASSERT arg_val > 0;
			ASSERT arg_sender <> arg_receiver;
			ASSERT arg_sender >= 0 ;
			ASSERT arg_receiver > 0;
			ASSERT (arg_sender = 0 AND arg_min = 0) OR (arg_sender > 0);

			ASSERT char_length(arg_uid) > 0 AND char_length(trim(arg_uid)) = char_length(arg_uid);

			PERFORM internal_func_assert_isolation('serializable');

			IF EXISTS (SELECT TRUE FROM tab_transfers WHERE col_uid = arg_uid) THEN
				-- RAISE NOTICE 'transfer col_uid % exists', arg_uid;
				RETURN -1;
			END IF;

			PERFORM pg_advisory_xact_lock_shared(arg_receiver);

			IF arg_sender > 0 THEN 
			
				PERFORM pg_advisory_xact_lock(arg_sender);
				-- RAISE NOTICE 'sender exclusive lock acquired %', arg_sender;
				
				SELECT internal_func_balance(arg_sender) INTO var_sum_balance;
				ASSERT var_sum_balance >= 0;

				var_new_balance := var_sum_balance - arg_val;
		
				IF var_new_balance < arg_min THEN
					-- RAISE NOTICE 'insufficient sender balance % %', arg_sender, var_new_balance;
					RETURN -2;
				END IF;

			END IF;

			INSERT INTO tab_transfers(col_uid, col_src, col_dst, col_val) VALUES (arg_uid, arg_sender, arg_receiver, arg_val) ON CONFLICT (col_uid) DO NOTHING RETURNING col_seq INTO var_inserted_num;
			IF var_inserted_num IS NULL THEN
				-- RAISE NOTICE 'transfer col_uid % exists', arg_uid;
				RETURN -3;
			END IF;
			ASSERT var_inserted_num > 0;

			IF arg_sender > 0 THEN 
				UPDATE tab_balances SET col_seq = var_inserted_num, col_val = var_new_balance WHERE col_acc = arg_sender RETURNING TRUE INTO var_updated;
				ASSERT var_updated = TRUE;
				-- RAISE NOTICE 'transfer from % to % value % col_uid %', arg_sender, arg_receiver, arg_val, arg_uid;
			END IF;

			RETURN var_inserted_num;

	END;
	$$ LANGUAGE plpgsql;

COMMIT;