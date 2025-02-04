library(gtfsrouter)
library(lubridate)

args <- commandArgs(trailingOnly = TRUE)

package_path <- args[1]

#gtfsr <- readRDS(gtfs_file_path)


gtfs_file_path = "C:/Users/dubrocac/.mobility/data/gtfs/1b3e497042c10298355caebe9c2dd0d2-f47807b294f863252c2da5e52a82a0da_GTFS_haute_savoie.zip"
gtfs <- extract_gtfs (gtfs_file_path)
print("GTFS read")
gtfs <- gtfs_timetable (gtfs, day = "Mon") # A pre-processing step to speed up queries

# Run routing tests for this GTFS
result <- gtfs_route (gtfs, from="Le Sougey", to="ANNEMASSE Gare", start_time = 7*3600)
print(result)
print(length(result))
class(result)


gtfs_file_path = "C:/Users/dubrocac/.mobility/data/projects/gtfs-vallorbe-pontarlier-corr.zip"
gtfs <- extract_gtfs (gtfs_file_path)
print("GTFS read")
gtfs <- gtfs_timetable (gtfs, date = "20240623") # A pre-processing step to speed up queries

# Run routing tests for this GTFS
result <- gtfs_route (gtfs, from="Jougne", to="Vallorbe", start_time = 8*3600)
print(result)
print(length(result))



gtfs_file_path = "C:/Users/dubrocac/.mobility/data/projects/export-ter-gtfs-2024-06-12-edited-haut-doubs-corr.zip"
gtfs <- extract_gtfs (gtfs_file_path)
print("GTFS read")
print(gtfs)
print("Building timetable")
gtfs <- gtfs_timetable (gtfs, day = "Tue") # A pre-processing step to speed up queries

# Run routing tests for this GTFS
result <- gtfs_route (gtfs, from="Frasne", to="Vallorbe", start_time = 8*3600)
print(result)
print(length(result))
