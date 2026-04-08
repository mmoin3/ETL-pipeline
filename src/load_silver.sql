WITH daily_model_holdings AS (
SELECT
	NULLIF(ltrim("Sedol Number",''''),'') AS sedol,
	NULLIF(ltrim("CUSIP Number",''''),'') AS cusip,
	*
	FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\daily_model_holdings")
)
SELECT
	"Fund" AS fund,
	"Asset Class" AS asset_class,
	"Bloomberg Ticker" AS bbg_ticker,
	"Bloomberg Expanded Ticker" AS bbg_exch_ticker,
	CASE
		WHEN "Bloomberg Ticker" IS NULL
			THEN concat("Currency Code",' Curncy')
		ELSE CONCAT(COALESCE("Bloomberg Expanded Ticker","Bloomberg Ticker"),' Equity')
	END AS "bbg_identifier",
	*
FROM daily_model_holdings
WHERE asset_class = 'OPTIONS' AND "Currency code" NOT IN ('USD','CAD')

SELECT DISTINCT "Security Name","Local Strike Price","Asset Class", "Currency code" FROM daily_model_holdings
WHERE "Asset Class" = 'OPTIONS' AND "Currency code" NOT IN ('USD','CAD');

SELECT *
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\pcf_inav_baskets")
--WHERE "DESCRIPTION" = 'SWISSCOM AG'

SELECT *
FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\custody_positions")
WHERE "Security Name" = 'ORANGE'


--SELECT
--	CASE
--		WHEN "Bloomberg Ticker" IS NULL
--			THEN concat("Currency Code",' Curncy')
--		ELSE CONCAT(COALESCE("Bloomberg Expanded Ticker","Bloomberg Ticker"),' Equity')
--	END AS "bbg_ticker",	
--FROM daily_model_holdings;


--WHERE "Period End Date" = '04/06/2026'
--AND "Bloomberg Expanded Ticker" = 'BKNG US'

--WHERE "Bloomberg Expanded Ticker" = "";


--AND "Bloomberg Expanded Ticker" IS NULL
--SELECT
----    CAST(LOCAL_PRICE AS DOUBLE) AS local_price,
----    CAST(CREATION_UNIT_SIZE AS INT) AS pnu_size,
----    LTRIM(TRIM(CUSIP), '''') AS clean_cusip,
----    LTRIM(TRIM(SEDOL), '''') AS clean_sedol,
----    CASE 
----        WHEN LTRIM(TRIM(TICKER), '''') IS NOT NULL 
----             AND LTRIM(TRIM(TICKER), '''') != '' 
----        THEN CONCAT(LTRIM(TRIM(TICKER), ''''), ' Equity')
----    END AS bbg_ticker,
--    *,
--FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\pcf_inav_baskets") AS p;
--SELECT
--	"Fund",
--	"Security Name",
--	"Asset Class",
--	"Bloomberg Expanded Ticker",
--	"Exchange Ticker",
--	"ISIN Number",
--	"Underlying ISIN",
--	"Period End Date",
--FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\daily_model_holdings")
--WHERE 'Asset Class' = "OPTIONS";
--FROM delta_scan("C:\Users\mmoin\PYTHON PROJECTS\DataWareHouse\bronze\accounting_nav_records")

--ORDER BY "Date:" DESC;