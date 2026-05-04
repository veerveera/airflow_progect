--dm.dm_acoount_turnover_f - витрина оборотов

CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
	
	DELETE FROM dm.dm_account_turnover_f tr WHERE tr.on_date = i_OnDate;
	
	INSERT INTO dm.dm_account_turnover_f 
	SELECT 
		i_OnDate,
		acc.account_rk,
		SUM(acc.cr_amount)::NUMERIC(23,8) AS crefit_amount,
		(SUM(acc.cr_amount) * COALESCE(ex.reduced_cource, 1))::NUMERIC(23,8) AS credit_amount_rub,
		SUM(acc.db_amount)::NUMERIC(23,8) AS debet_amount,
		(SUM(acc.db_amount) * COALESCE(ex.reduced_cource, 1))::NUMERIC(23,8) AS debet_amount_rub
	FROM (
		SELECT ft.credit_account_rk AS account_rk, ft.credit_amount AS cr_amount, 0::float AS db_amount
		FROM ds.ft_posting_f ft
		WHERE ft.oper_date = i_OnDate
		UNION ALL 
		SELECT ft.debet_account_rk AS account_rk, 0::float AS cr_amount, ft.debet_amount AS db_amount
		FROM ds.ft_posting_f ft
		WHERE ft.oper_date = i_OnDate
		) acc
	LEFT JOIN ds.md_account_d val ON acc.account_rk = val.account_rk AND i_OnDate >= val.data_actual_date AND i_OnDate <= val.data_actual_end_date
	LEFT JOIN ds.md_exchange_rate_d ex ON val.currency_rk = ex.currency_rk AND i_OnDate >= ex.data_actual_date 
	AND (i_OnDate <= ex.data_actual_end_date OR ex.data_actual_end_date IS NULL)
	GROUP BY acc.account_rk, ex.reduced_cource;
	
END
$$

--dm.dm_account_balance_f - витрина остатков

CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
BEGIN
	DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;

	IF i_OnDate = '2017-12-31'::DATE THEN 
		INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
		SELECT ft.on_date, ft.account_rk, ft.balance_out, (ft.balance_out * COALESCE (ex.reduced_cource, 1))::NUMERIC(23,8)
		FROM ds.ft_balance_f ft
		
		LEFT JOIN ds.md_exchange_rate_d ex ON ft.currency_rk = ex.currency_rk AND ft.on_date >= ex.data_actual_date
		AND (ft.on_date <= ex.data_actual_end_date OR ex.data_actual_end_date IS NULL)
		WHERE ft.on_date = i_OnDate;
	ELSE
		INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
		SELECT i_OnDate, acc.account_rk,
		CASE WHEN acc.char_type = 'A' THEN COALESCE(prev.balance_out, 0) + COALESCE(tr.debet_amount, 0) - COALESCE(tr.credit_amount, 0)
			 WHEN acc.char_type = 'П' THEN COALESCE(prev.balance_out, 0) - COALESCE(tr.debet_amount, 0) + COALESCE(tr.credit_amount, 0)
		END AS balance_out,
		CASE WHEN acc.char_type = 'A' THEN COALESCE(prev.balance_out_rub, 0) + COALESCE(tr.debet_amount_rub, 0) - COALESCE(tr.credit_amount_rub, 0)
			 WHEN acc.char_type = 'П' THEN COALESCE(prev.balance_out_rub, 0) - COALESCE(tr.debet_amount_rub, 0) + COALESCE(tr.credit_amount_rub, 0)
		END AS balance_out_rub
		FROM ds.md_account_d acc
		
		LEFT JOIN dm.dm_account_balance_f prev ON prev.on_date = i_OnDate - INTERVAL '1 day' AND prev.account_rk = acc.account_rk
		LEFT JOIN dm.dm_account_turnover_f tr ON tr.on_date = i_OnDate AND tr.account_rk = acc.account_rk
		
		WHERE i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;
    END IF;

END
$$ 

--ill_f101_round_f - процедура расчета
CREATE OR REPLACE PROCEDURE dm.fill_f101_round_f (i_OnDate DATE)
LANGUAGE plpgsql
AS $$
DECLARE 
	v_FromDate DATE; 
	v_ToDate DATE;
	v_CalcDate DATE;
BEGIN 
	v_CalcDate := '2018-02-01';
	v_FromDate := DATE_TRUNC('month', v_CalcDate - INTERVAL '1 month');
	v_ToDate := (DATE_TRUNC ('month', v_CalcDate) - INTERVAL '1 day');
	
	DELETE FROM dm.dm_f101_round_f WHERE to_date = v_ToDate;

	INSERT into dm.dm_f101_round_f (
	from_date, to_date, chapter, ledger_account, characteristic, 
	balance_in_rub, balance_in_val, balance_in_total, 
	turn_deb_rub, turn_deb_val, turn_deb_total, 
	turn_cre_rub, turn_cre_val, turn_cre_total, 
	balance_out_rub, balance_out_val, balance_out_total)

	SELECT v_FromDate, v_ToDate, acc_s.chapter,
	LEFT(acc.account_number, 5) AS ledger_account,
	acc.char_type AS characteristic,
	SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN bal_from.balance_out_rub ELSE 0 END) AS balance_in_rub,
	SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN bal_from.balance_out_rub ELSE 0 END) AS balance_in_val,
	SUM(COALESCE(bal_from.balance_out_rub, 0)) AS balance_in_total,
	
	SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN turn.debet_amount_rub ELSE 0 END) AS turn_deb_rub,
	SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN turn.debet_amount_rub ELSE 0 END) AS turn_deb_val,
	SUM(COALESCE(turn.debet_amount_rub, 0)) AS turn_deb_total,
	
	SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN turn.credit_amount_rub ELSE 0 END) AS turn_cre_rub,
	SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN turn.credit_amount_rub ELSE 0 END) AS turn_cre_val,
	SUM(COALESCE(turn.credit_amount_rub, 0)) AS turn_cre_total,
	
	SUM(CASE WHEN acc.currency_code IN ('810', '643') THEN bal_last.balance_out_rub ELSE 0 END) AS balance_out_rub,
	SUM(CASE WHEN acc.currency_code NOT IN ('810', '643') THEN bal_last.balance_out_rub ELSE 0 END) AS balance_out_val,
	SUM(COALESCE(bal_last.balance_out_rub, 0)) AS balance_out_total
	
	FROM ds.md_account_d acc
	
	LEFT JOIN ds.md_ledger_account_s acc_s ON acc_s.ledger_account = LEFT(acc.account_number, 5)::INTEGER
	LEFT JOIN dm.dm_account_balance_f bal_from ON acc.account_rk = bal_from.account_rk AND bal_from.on_date = v_FromDate - INTERVAL '1 day'
	LEFT JOIN dm.dm_account_balance_f bal_last ON acc.account_rk = bal_last.account_rk AND bal_last.on_date = v_ToDate
	LEFT JOIN dm.dm_account_turnover_f turn ON acc.account_rk = turn.account_rk AND turn.on_date BETWEEN v_FromDate AND v_ToDate
	
	WHERE acc.data_actual_date <= v_ToDate AND acc.data_actual_end_date >= v_FromDate
	
	GROUP BY acc_s.chapter, LEFT(acc.account_number, 5), acc.char_type;
	
END;
$$


SELECT count(*) 
FROM ds.md_account_d 
WHERE '2018-02-01' BETWEEN data_actual_date AND data_actual_end_date;
