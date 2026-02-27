library(gtfsrouter)
library(lubridate)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]

#gtfsr <- readRDS(gtfs_file_path)


gtfs_file_path = "D:/mobility-data/gtfs/2fe7a81cf3588467223c62ce1f67a2df-2a105e398629c91561f0d83c129bd003_downloadproviderHAUTE_SAVOIEdataFormatGTFSdataProfilOPENDATA.zip"
gtfs <- extract_gtfs (gtfs_file_path)
print("GTFS read")
gtfs <- gtfs_timetable (gtfs, day = "Mon") # A pre-processing step to speed up queries

# Run routing tests for this GTFS
result <- gtfs_route (gtfs, from="Le Sougey", to="Centre de Transfusion", start_time = 7*3600)
print(result)
print(length(result))
class(result)

gtfs_file_path = "D:/mobility-data/gtfs/465877e6eca4d1545e81d6e97eb93719-a161391d60620240d7ae4ee37235fbc3_gtfs_complete.zip"
print("GTFS Suisse entière")
gtfs <- extract_gtfs (gtfs_file_path)
print("GTFS read")
gtfs <- gtfs_timetable (gtfs, day = "Tue") # A pre-processing step to speed up queries
#print("Kept Tuesdays")

# Run routing tests for this GTFS
result <- gtfs_route (gtfs, from="Plan-les-Ouates, ZIPLO", to="Genève, gare Cornavin", start_time = 7*3600)
print(result)
print(length(result))
class(result)





#gtfs_file_path = "D:/mobility-data/gtfs/gtfs-vallorbe-pontarlier-corr.zip"
#gtfs <- extract_gtfs (gtfs_file_path)
#print("GTFS read")
#gtfs <- gtfs_timetable (gtfs, date = "20240623") # A pre-processing step to speed up queries

# Run routing tests for this GTFS
#result <- gtfs_route (gtfs, from="Jougne", to="Vallorbe", start_time = 8*3600)
#print(result)
#print(length(result))



#gtfs_file_path = "D:/mobility-data/gtfs/export-ter-gtfs-2024-06-12-edited-haut-doubs-corr.zip"
#gtfs <- extract_gtfs (gtfs_file_path)
#print("GTFS read")
#print(gtfs)
#print("Building timetable")
#gtfs <- gtfs_timetable (gtfs, day = "Tue") # A pre-processing step to speed up queries

# Run routing tests for this GTFS
#result <- gtfs_route (gtfs, from="Frasne", to="Vallorbe", start_time = 8*3600)
#print(result)
#print(length(result))
