"""
Print statistics (rows, columns, size) for W&B artifacts used to create the main flood dataset.

Uses artifact metadata only—no download of full data. Run from repo root:
    uv run python data_exploration/wandb_artifact_stats.py
"""

import wandb

PROJECT = "flood-forecasting"

# Artifacts that feed into or are the main flood_model pipeline
ARTIFACTS = [
    # Final tables (what we call "main" in wandb)
    "flood-dataset",
    "flood-dataset-daily",
    # Raw tables used to build them
    "raw-site-metadata",
    "raw-streamflow-15min",
    "raw-streamflow-daily",
    "raw-nldas3-forcing",
    "raw-watershed-mapping",
]


def main() -> None:
    api = wandb.Api()
    rows = []

    for name in ARTIFACTS:
        try:
            artifact = api.artifact(f"{PROJECT}/{name}:latest")
            meta = artifact.metadata or {}
            row_count = meta.get("row_count") or meta.get("rows")  # some use row_count
            size_mb = meta.get("file_size_mb") or meta.get("size_mb")
            schema = meta.get("schema")
            n_cols = len(schema) if isinstance(schema, dict) else None
            size_str = f"{float(size_mb):,.1f}" if size_mb is not None else "—"
            row_str = f"{int(row_count):,}" if row_count is not None else "—"
            rows.append(
                {
                    "artifact": name,
                    "rows": row_str,
                    "columns": str(n_cols) if n_cols is not None else "—",
                    "size_mb": size_str,
                }
            )
        except Exception as e:
            rows.append(
                {
                    "artifact": name,
                    "rows": "—",
                    "columns": "—",
                    "size_mb": f"error: {e!r}",
                }
            )

    # Print as a simple table
    headers = ["artifact", "rows", "columns", "size_mb"]
    if not rows:
        print("No artifact metadata found.")
        return
    col_widths = [
        max(len(str(r[k])) for r in rows) + 2 for k in headers
    ]
    col_widths[0] = max(col_widths[0], len("artifact") + 2)
    for i, h in enumerate(headers):
        col_widths[i] = max(col_widths[i], len(h) + 2)

    def line() -> str:
        return "+" + "+".join("-" * w for w in col_widths) + "+"

    print(line())
    print("|" + "|".join(h.ljust(col_widths[i]) for i, h in enumerate(headers)) + "|")
    print(line())
    for r in rows:
        print(
            "|"
            + "|".join(
                str(r[k]).ljust(col_widths[i]) for i, k in enumerate(headers)
            )
            + "|"
        )
    print(line())


if __name__ == "__main__":
    main()
