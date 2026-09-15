"""Small standard-library Trino client used by local verification scripts."""

import json
import os
import urllib.request


def query(sql, user="week1-check"):
    base = os.getenv("TRINO_URL", "http://localhost:8081")
    request = urllib.request.Request(
        base + "/v1/statement", data=sql.encode(),
        headers={"X-Trino-User": user, "Content-Type": "text/plain"},
    )
    columns, rows = [], []
    while True:
        with urllib.request.urlopen(request, timeout=120) as response:
            result = json.load(response)
        if "error" in result:
            raise RuntimeError(result["error"]["message"])
        if "columns" in result:
            columns = [c["name"] for c in result["columns"]]
        rows.extend(result.get("data", []))
        if "nextUri" not in result:
            return [dict(zip(columns, row)) for row in rows]
        # Server returns its internal address when accessed through a proxy.
        from urllib.parse import urlsplit
        request = urllib.request.Request(
            base + urlsplit(result["nextUri"]).path,
            headers={"X-Trino-User": user},
        )


if __name__ == "__main__":
    import sys
    print(json.dumps(query(sys.argv[1]), ensure_ascii=False, indent=2))
