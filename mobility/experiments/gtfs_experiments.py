import gtfs_kit

k = gtfs_kit.read_feed("https://app.mecatran.com/utw/ws/gtfsfeed/static/lio?apiKey=2b160d626f783808095373766f18714901325e45&type=gtfs_lio", "m")

trips = k.get_trips()
dates = k.get_dates()
routes = k.get_routes()
shapes = k.get_shapes(as_gdf=True)
shapes.plot()

t = gtfs_kit.routes.build_route_timetable(k, '452', ["20251020"])

ts = k.compute_trip_stats(["452"])
rs = gtfs_kit.routes.compute_route_stats(feed=k, dates=["20251020"], trip_stats_subset=ts)

