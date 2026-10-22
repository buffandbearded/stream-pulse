"""Print the dbt marts from data/warehouse.duckdb (read-only)."""
import duckdb

con = duckdb.connect("data/warehouse.duckdb", read_only=True)

print("== staging row count ==")
print(con.sql("SELECT count(*) AS events, count(DISTINCT event_id) AS unique_events FROM stg_wiki_events"))
print("== fct_edits_per_minute (latest 8 minutes) ==")
print(con.sql("SELECT * FROM fct_edits_per_minute ORDER BY event_minute DESC LIMIT 8"))
print("== mart_wiki_leaderboard (top 8) ==")
print(con.sql("SELECT * FROM mart_wiki_leaderboard LIMIT 8"))
print("== mart_top_editors (top 8) ==")
print(con.sql("SELECT user_name, edits, wikis_touched, net_bytes FROM mart_top_editors LIMIT 8"))
con.close()
