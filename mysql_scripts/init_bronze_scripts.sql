--stored procedure
DELIMITER $$

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
BEGIN

  -- Truncate and load sales_details
  TRUNCATE TABLE bronze.sales_details;
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/sales_details.csv'
  INTO TABLE bronze.sales_details
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (@sls_ord_num, @sls_prd_key, @sls_cust_id, @sls_order_dt, @sls_ship_dt, @sls_due_dt, @sls_sales, @sls_quantity, @sls_price)
  SET
    sls_ord_num   = NULLIF(@sls_ord_num, ''),
    sls_prd_key   = NULLIF(@sls_prd_key, ''),
    sls_cust_id   = NULLIF(@sls_cust_id, ''),
    sls_order_dt  = NULLIF(@sls_order_dt, ''),
    sls_ship_dt   = NULLIF(@sls_ship_dt, ''),
    sls_due_dt    = NULLIF(@sls_due_dt, ''),
    sls_sales     = NULLIF(@sls_sales, ''),
    sls_quantity  = NULLIF(@sls_quantity, ''),
    sls_price     = NULLIF(@sls_price, '');

  -- Truncate and load prd_info
  TRUNCATE TABLE bronze.prd_info;
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/prd_info.csv'
  INTO TABLE bronze.prd_info
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (@prd_id, @prd_key, @prd_nm, @prd_cost, @prd_line, @prd_start_dt, @prd_end_dt)
  SET
    prd_id       = NULLIF(@prd_id, ''),
    prd_key      = NULLIF(@prd_key, ''),
    prd_nm       = NULLIF(@prd_nm, ''),
    prd_cost     = NULLIF(@prd_cost, ''),
    prd_line     = NULLIF(@prd_line, ''),
    prd_start_dt = NULLIF(@prd_start_dt, ''),
    prd_end_dt   = NULLIF(@prd_end_dt, '');

  -- Truncate and load PX_CAT_GIV2
  TRUNCATE TABLE bronze.PX_CAT_GIV2;
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/PX_CAT_GIV2.csv'
  INTO TABLE bronze.PX_CAT_GIV2
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (@ID, @CAT, @SUBCAT, @MAINTENANCE)
  SET
    ID           = NULLIF(@ID, ''),
    CAT          = NULLIF(@CAT, ''),
    SUBCAT       = NULLIF(@SUBCAT, ''),
    MAINTENANCE  = NULLIF(@MAINTENANCE, '');

  -- Truncate and load cust_az12
  TRUNCATE TABLE bronze.cust_az12;
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cust_az12.csv'
  INTO TABLE bronze.cust_az12
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (@CID, @BDATE, @GEN)
  SET
    CID   = NULLIF(@CID, ''),
    BDATE = NULLIF(@BDATE, ''),
    GEN   = NULLIF(@GEN, '');

  -- Truncate and load LOC_A101
  TRUNCATE TABLE bronze.LOC_A101;
  LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/LOC_A101.csv'
  INTO TABLE bronze.LOC_A101
  FIELDS TERMINATED BY ',' ENCLOSED BY '"'
  LINES TERMINATED BY '\r\n'
  IGNORE 1 LINES
  (@CID, @CNTRY)
  SET
    CID   = NULLIF(@CID, ''),
    CNTRY = NULLIF(@CNTRY, '');

END$$

DELIMITER ;
