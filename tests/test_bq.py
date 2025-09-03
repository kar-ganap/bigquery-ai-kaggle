from google.cloud import bigquery

# Client will auto-pick up GOOGLE_APPLICATION_CREDENTIALS from env
client = bigquery.Client()

query = """
SELECT name, COUNT(*) AS name_count
FROM `bigquery-public-data.usa_names.usa_1910_2013`
GROUP BY name
ORDER BY name_count DESC
LIMIT 5
"""

results = client.query(query).result()
for row in results:
    print(f"{row.name}: {row.name_count}")

