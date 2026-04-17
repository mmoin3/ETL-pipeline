SELECT
	CASE
		WHEN "Asset Class" <> 'CASH EQUIVALENT'
		THEN COALESCE("Bloomberg Ticker","Currency Code")
	END AS bbg_ticker,
	*
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\daily_model_holdings");

SELECT DISTINCT Fund
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\daily_model_holdings");

SELECT *
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\all_positions");


SELECT DISTINCT "Security Name","Local Strike Price","Asset Class", "Currency code" FROM daily_model_holdings
WHERE "Asset Class" = 'OPTIONS' AND "Currency code" NOT IN ('USD','CAD');

SELECT distinct
	REPLACE(LTRIM(TICKER_1,''''),'.','/') AS fund_code,
	left(SS_LONG_CODE,4) AS ss_code,
	CASE
		WHEN len(SS_LONG_CODE) > 6 THEN SUBSTRING(SS_LONG_CODE,5,1)
		ELSE NULL
	END AS fund_class
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\pcf_inav_baskets");
--WHERE "DESCRIPTION" = 'SWISSCOM AG'

SELECT *
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\custody_positions")
WHERE "Security Name" = 'ORANGE'

WITH cleaned_base AS (
    SELECT 
        COALESCE("State Street Code", ss_code) AS ss_id,
        fund_code,
        settlement_instruction,
        DDA
    FROM read_csv('C:/Users/mmoin/PYTHON PROJECTS/DataWareHouse/data/0_raw data/harvest_funds_data_all.csv')
)
SELECT DISTINCT
    f1.ss_id,
    STRING_SPLIT(f1.fund_code, '/')[1] AS fund_code,
    STRING_SPLIT(f1.fund_code, '/')[2] AS fund_class,
    f1.settlement_instruction AS account_type,
    CAST (COALESCE (f1.DDA, f2.DDA) AS string) AS DDA
FROM cleaned_base f1
LEFT JOIN cleaned_base f2 
    ON f1.ss_id = f2.ss_id AND f2.DDA IS NOT NULL;

WITH harvest_funds AS ( SELECT * FROM read_csv("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\data\0_raw data\harvest_funds_data_all.csv"))
SELECT DISTINCT
	STRING_SPLIT (f1."fund_code",'/')[1] AS fund_code,
	
FROM harvest_funds;

