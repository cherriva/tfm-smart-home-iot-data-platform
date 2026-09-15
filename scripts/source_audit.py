"""Show the real/synthetic provenance kept outside the Grafana presentation."""

from trino_client import query


SQL = """
SELECT layer, source, events
FROM (
    SELECT 'Bronze' AS layer, coalesce(source, 'unknown') AS source, count(*) AS events
    FROM tfm.iot_bronze.matter_events
    GROUP BY source
    UNION ALL
    SELECT 'Silver', coalesce(source, 'unknown'), count(*)
    FROM tfm.iot_silver.matter_events
    GROUP BY source
    UNION ALL
    SELECT 'Gold', coalesce(source, 'unknown'), sum(event_count)
    FROM tfm.iot_gold.entity_5m
    GROUP BY source
)
ORDER BY CASE layer WHEN 'Bronze' THEN 1 WHEN 'Silver' THEN 2 ELSE 3 END, source
"""


def main() -> None:
    rows = query(SQL, user="source-audit")
    print(f"{'CAPA':<10} {'ORIGEN':<12} {'EVENTOS':>12}")
    print("-" * 36)
    for row in rows:
        print(f"{row['layer']:<10} {row['source']:<12} {row['events']:>12}")


if __name__ == "__main__":
    main()
