CREATE TABLE [dbo].[FactTaxiDaily] (

	[date_key] int NULL, 
	[zone_key] int NULL, 
	[fx_key] int NULL, 
	[trip_count] bigint NOT NULL, 
	[total_fare_usd] float NULL, 
	[total_fare_eur] float NULL, 
	[avg_fare_usd] float NULL, 
	[avg_trip_duration_min] float NULL, 
	[avg_trip_distance_mi] float NULL, 
	[total_passengers] int NULL
);