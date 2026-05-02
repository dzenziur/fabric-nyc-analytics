[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
shared fn_GetPage = (page as number) =>
let
    Source = Json.Document(
        Web.Contents(
            "https://api.openaq.org/v3/locations?limit=1000&page=" & Number.ToText(page),
            [Headers = [#"X-API-Key" = "e0cd56a66db9820576ca150ccbdd9462cdb6f41c6d69cc08880a63185b63a709"]]
        )
    ),
  Navigation = Source[results],
  #"Converted to table" = Table.FromList(Navigation, Splitter.SplitByNothing(), null, null, ExtraValues.Error),
  #"Expanded Column1 1" = Table.ExpandRecordColumn(#"Converted to table", "Column1", {"id", "name", "timezone", "country", "coordinates"}, {"id", "name", "timezone", "country", "coordinates"}),
  #"Expanded country" = Table.ExpandRecordColumn(#"Expanded Column1 1", "country", {"id", "name"}, {"id.1", "name.1"}),
  #"Renamed columns" = Table.RenameColumns(#"Expanded country", {{"id.1", "country_id"}, {"name.1", "country_name"}}),
  #"Expanded coordinates" = Table.ExpandRecordColumn(#"Renamed columns", "coordinates", {"latitude", "longitude"}, {"latitude", "longitude"}),
  #"Renamed columns 1" = Table.RenameColumns(#"Expanded coordinates", {{"id", "location_id"}, {"name", "location_name"}}),
  #"Changed column type" = Table.TransformColumnTypes(#"Renamed columns 1", {{"location_id", Int64.Type}, {"location_name", type text}, {"timezone", type text}, {"country_id", type text}, {"country_name", type text}, {"latitude", type number}, {"longitude", type number}})
in
    #"Changed column type";
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "openaq_locations_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared openaq_locations = let
    pages = {1..5},
    results = List.Transform(pages, each fn_GetPage(_)),
    combined = Table.Combine(results)
in
    combined;
shared openaq_locations_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "d5f75821-ae8f-4a0a-b235-74982716aa0b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c9741aec-56ed-41b9-9025-c45ee072256d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_air_quality", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
