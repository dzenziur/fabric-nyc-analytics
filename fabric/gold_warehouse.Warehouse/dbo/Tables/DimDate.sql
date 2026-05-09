CREATE TABLE [dbo].[DimDate] (

	[date_key] int NOT NULL, 
	[date] date NOT NULL, 
	[year] int NOT NULL, 
	[quarter] int NOT NULL, 
	[month] int NOT NULL, 
	[month_name] varchar(max) NOT NULL, 
	[week_of_year] int NOT NULL, 
	[day_of_month] int NOT NULL, 
	[day_of_week] int NULL, 
	[day_name] varchar(max) NOT NULL, 
	[is_weekend] bit NULL
);