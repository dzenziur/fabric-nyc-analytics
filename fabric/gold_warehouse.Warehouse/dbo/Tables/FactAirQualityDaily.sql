CREATE TABLE [dbo].[FactAirQualityDaily] (

	[date_key] int NULL, 
	[location_id] bigint NULL, 
	[city] varchar(max) NULL, 
	[country] varchar(max) NULL, 
	[latitude] float NULL, 
	[longitude] float NULL, 
	[parameter] varchar(max) NULL, 
	[avg_value] float NULL, 
	[max_value] float NULL, 
	[min_value] float NULL, 
	[measurement_count] bigint NOT NULL
);