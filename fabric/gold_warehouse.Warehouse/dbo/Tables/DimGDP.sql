CREATE TABLE [dbo].[DimGDP] (

	[gdp_key] int NOT NULL, 
	[country_code] varchar(max) NULL, 
	[country_name] varchar(max) NULL, 
	[year] int NULL, 
	[gdp_usd] float NULL, 
	[gdp_trillion_usd] float NULL
);