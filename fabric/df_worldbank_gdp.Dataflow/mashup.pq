[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared worldbank_gdp = let
  currentYear = Date.Year(DateTime.LocalNow()) - 1,
  url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.MKTP.CD?format=json&per_page=20000&date=2000:" & Text.From(currentYear),
  Source = Json.Document(Web.Contents(url)),
  #"Converted to table" = Table.FromList(Source, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  Navigation = #"Converted to table"{1}[Column1],
  #"Converted to table 1" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1" = Table.ExpandRecordColumn(#"Converted to table 1", "Column1", {"country", "date", "value"}, {"country", "date", "value"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded Column1", "country", {"id", "value"}, {"id", "value.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded country", {{"id", "country_code"}, {"value.1", "country_name"}, {"date", "year"}, {"value", "gdp_usd"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns", {{"country_code", type text}, {"country_name", type text}, {"year", Int64.Type}, {"gdp_usd", type number}}),
  #"Removed blank rows" = Table.SelectRows(#"Changed column type", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null})))
in
  #"Removed blank rows";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "d5f75821-ae8f-4a0a-b235-74982716aa0b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c9741aec-56ed-41b9-9025-c45ee072256d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_gdp", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
