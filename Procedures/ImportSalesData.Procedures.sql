DELIMITER $$

--
-- Create procedure `ImportSalesData`
--
CREATE PROCEDURE ImportSalesData(_JSONString json,
OUT _Result int)
BEGIN

	DECLARE errno int;
	DECLARE errcode char(5) DEFAULT '00000';
	DECLARE msg text;
	DECLARE EXIT HANDLER FOR SQLEXCEPTION
	BEGIN
		GET CURRENT DIAGNOSTICS CONDITION 1 errno = MYSQL_ERRNO, errcode = RETURNED_SQLSTATE, msg = MESSAGE_TEXT;
		SET _Result = errno; -- return mysql error no.
		ROLLBACK;
	END;
	-- SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	START TRANSACTION;

		INSERT INTO sales
		(
			Region,
			Country,
			ItemType,
			SalesChannel,
			OrderPriority,
			OrderDate,
			OrderID,
			ShipDate,
			UnitSold,
			UnitPrice,
			UnitCost,
			TotalRevenue,
			TotalCost,
			TotalProfit
		)
		WITH CTE AS (
			SELECT
				p.Region,
				p.Country,
				p.ItemType,
				p.SalesChannel,
				p.OrderPriority,
				DATE_FORMAT(p.OrderDate, '%Y-%m-%d') AS OrderDate,
				p.OrderID,
				DATE_FORMAT(p.ShipDate, '%Y-%m-%d') AS ShipDate,
				p.UnitSold,
				p.UnitPrice,
				p.UnitCost,
				p.TotalRevenue,
				p.TotalCost,
				p.TotalProfit
			FROM JSON_TABLE (_JSONString, '$[*]' COLUMNS (
					Region varchar(50) PATH '$.Column0',
					Country varchar(50) PATH '$.Column1',
					ItemType varchar(50) PATH '$.Column2',
					SalesChannel varchar(50) PATH '$.Column3',
					OrderPriority varchar(50) PATH '$.Column4',
					OrderDate varchar(50) PATH '$.Column5',
					OrderID varchar(50) PATH '$.Column6',
					ShipDate varchar(50) PATH '$.Column7',
					UnitSold varchar(50) PATH '$.Column8',
					UnitPrice varchar(50) PATH '$.Column9',
					UnitCost varchar(50) PATH '$.Column10',
					TotalRevenue varchar(50) PATH '$.Column11',
					TotalCost varchar(50) PATH '$.Column12',
					TotalProfit varchar(50) PATH '$.Column13'
 					)) p
					WHERE p.Region != 'Region' 
		)
		SELECT 
				c.Region,
				c.Country,
				c.ItemType,
				c.SalesChannel,
				c.OrderPriority,
				c.OrderDate,
				c.OrderID,
				c.ShipDate,
				c.UnitSold,
				c.UnitPrice,
				c.UnitCost,
				c.TotalRevenue,
				c.TotalCost,
				c.TotalProfit
		FROM CTE c;



		IF ROW_COUNT() > 0 THEN	
			BEGIN
				SET _Result = 1; -- successful upload
			END;
		ELSE
			BEGIN
				SET _Result = 0; -- failed to uipload
			END;
		END IF;

		

	COMMIT WORK;

END
$$

DELIMITER ;