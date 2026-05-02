[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "Query_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared ecb_fx_rates = let
  Source = Csv.Document(Web.Contents("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"), [Delimiter = ",", Columns = 32, QuoteStyle = QuoteStyle.None]),
  #"Promoted headers" = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"KEY", type text}, {"FREQ", type text}, {"CURRENCY", type text}, {"CURRENCY_DENOM", type text}, {"EXR_TYPE", type text}, {"EXR_SUFFIX", type text}, {"TIME_PERIOD", type date}, {"OBS_VALUE", type number}, {"OBS_STATUS", type text}, {"OBS_CONF", type text}, {"OBS_PRE_BREAK", type text}, {"OBS_COM", type text}, {"TIME_FORMAT", type duration}, {"BREAKS", type text}, {"COLLECTION", type text}, {"COMPILING_ORG", type text}, {"DISS_ORG", type text}, {"DOM_SER_IDS", type text}, {"PUBL_ECB", type text}, {"PUBL_MU", type text}, {"PUBL_PUBLIC", type text}, {"UNIT_INDEX_BASE", type text}, {"COMPILATION", type text}, {"COVERAGE", type text}, {"DECIMALS", Int64.Type}, {"NAT_TITLE", type text}, {"SOURCE_AGENCY", type text}, {"SOURCE_PUB", type text}, {"TITLE", type text}, {"TITLE_COMPL", type text}, {"UNIT", type text}, {"UNIT_MULT", Int64.Type}}),
  #"Removed other columns" = Table.SelectColumns(#"Changed column type", {"TIME_PERIOD", "OBS_VALUE"}),
  #"Renamed columns" = Table.RenameColumns(#"Removed other columns", {{"TIME_PERIOD", "date"}, {"OBS_VALUE", "usd_eur_rate"}}),
  #"Changed column type 1" = Table.TransformColumnTypes(#"Renamed columns", {{"date", type date}, {"usd_eur_rate", type number}}),
  #"Removed blank rows" = Table.SelectRows(#"Changed column type 1", each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null})))
in
  #"Removed blank rows";
shared Query_DataDestination = let
  Pattern = Lakehouse.Contents([CreateNavigationProperties = false, EnableFolding = false, HierarchicalNavigation = null, EnableVorder = true, OutputMetadataRefresh = true]),
  Navigation_1 = Pattern{[workspaceId = "d5f75821-ae8f-4a0a-b235-74982716aa0b"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "c9741aec-56ed-41b9-9025-c45ee072256d"]}[Data],
  TableNavigation = Navigation_2{[Id = "bronze_fx_rates", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
