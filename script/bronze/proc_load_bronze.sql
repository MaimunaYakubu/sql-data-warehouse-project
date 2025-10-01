/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/



create or alter procedure bronze.load_bronze as
begin
	declare @load_bronze_start_time datetime, @load_bronze_end_time datetime
	declare @start_time datetime, @end_time datetime;
begin try
print '=======================================================';
set @load_bronze_start_time= GETDATE()
print 'Loading Bronze Layer';
print '=======================================================';

Print '-------------------------------------------------------';
Print 'Loading CRM Tables';
Print '-------------------------------------------------------';

set @start_time=GETDATE()
Print' >> Truncating table: BRONZE.crm_cust_info';
TRUNCATE TABLE BRONZE.crm_cust_info
Print '>> Inserting Data Into: BRONZE.crm_cust_info';
bULK INSERT BRONZE.crm_cust_info
FROM 'C:\dwh_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR =',',
TABLOCK
);
set @end_time =GETDATE()
print '>> Load DUration:' + cast (datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

print'>> ------------------------------------';

set @start_time=GETDATE()
Print' >> Truncating table: BRONZE.crm_prd_info';
TRUNCATE TABLE BRONZE.crm_prd_info

Print '>> Inserting Data Into: BRONZE.crm_prd_info';
bULK INSERT BRONZE.crm_prd_info
FROM 'C:\dwh_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR =',',
	TABLOCK
);
set @end_time =GETDATE()
print '>> Load DUration:' + cast (datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

print'>> ------------------------------------';
set @start_time=GETDATE()
Print' >> Truncating table: BRONZE.crm_sales_details';
TRUNCATE TABLE BRONZE.crm_sales_details

Print '>> Inserting Data Into: BRONZE.crm_sales_details';
bULK INSERT BRONZE.crm_sales_details
FROM 'C:\dwh_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR =',',
TABLOCK
);

set @end_time =GETDATE()
print '>> Load DUration:' + cast (datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

Print '-------------------------------------------------------';
Print 'Loading ERP Tables';
Print '-------------------------------------------------------';

set @start_time=GETDATE()
Print' >> Truncating table: BRONZE.erp_cust_az12';

TRUNCATE TABLE BRONZE.erp_cust_az12

Print '>> Inserting Data Into: BRONZE.erp_cust_az12';
bULK INSERT BRONZE.erp_cust_az12
FROM 'C:\dwh_project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR =',',
TABLOCK
);
set @end_time =GETDATE()
print '>> Load DUration:' + cast (datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

print'>> ------------------------------------';
set @start_time=GETDATE()
Print' >> Truncating table: BRONZE.erp_loc_a101';
TRUNCATE TABLE BRONZE.erp_loc_a101

Print '>> Inserting Data Into: BRONZE.erp_loc_a101';
bULK INSERT BRONZE.erp_loc_a101
FROM 'C:\dwh_project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR =',',
TABLOCK
);
set @end_time =GETDATE()
print '>> Load DUration:' + cast (datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

print'>> ------------------------------------';
set @start_time=GETDATE()
Print' >> Truncating table: BRONZE.erp_px_cat_g1v2';
TRUNCATE TABLE BRONZE.erp_px_cat_g1v2

Print '>> Inserting Data Into: BRONZE.erp_px_cat_g1v2';
bULK INSERT BRONZE.erp_px_cat_g1v2
FROM 'C:\dwh_project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
FIRSTROW = 2,
FIELDTERMINATOR =',',
TABLOCK
);
set @end_time =GETDATE()
print '>> Load DUration:' + cast (datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';

print'>> ------------------------------------';

end try
begin catch
Print '========================================================='
Print 'Error occured duild loading bronze layer'
Print 'Error message' + error_message()
Print 'Error message' + cast(error_number() as nvarchar)
Print 'Error message' + cast(error_state() as nvarchar)

Print '========================================================='
end catch
set @load_bronze_end_time =GETDATE()
print '=================================='
print 'Loading Bronze Layer is COmpleted'
print '>> Bronze Load DUration:' + cast (datediff(second, @load_bronze_start_time, @load_bronze_end_time) as nvarchar) + 'seconds';
print '=================================='
end
