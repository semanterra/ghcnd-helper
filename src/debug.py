from ghcnd.analysis.compute_singletons import compute_singletons
import ghcnd.process_station_csv
import altair as alt

# ghcnd.process_station_csv.main()

alt.renderers.enable("browser")
compute_singletons()