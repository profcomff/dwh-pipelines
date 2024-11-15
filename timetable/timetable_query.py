from .parsing_timetable_selected import parse_timetable

parse_timetable()

query = """
   SELECT *
   FROM STG_RASPHYSMSU.raw_html
   """
df = pd.read_sql(query, engine)