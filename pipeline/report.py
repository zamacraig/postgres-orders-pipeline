"""Generate Report.md with visualizations from database views."""

import os
import warnings
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import psycopg

warnings.filterwarnings("ignore", message="pandas only supports SQLAlchemy")

REPORT_DIR = Path(__file__).parent.parent / "reports"
REPORT_FILE = Path(__file__).parent.parent / "Report.md"

VIEWS = {
    "analytics": ["v_daily_metrics", "v_top_customers_by_spend", "v_top_skus"],
    "data_quality": ["v_dq_rejection_summary", "v_dq_duplicate_emails", "v_dq_orphan_orders", 
                     "v_dq_invalid_order_items", "v_dq_invalid_status"],
}


def get_connection():
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=int(os.environ.get("PGPORT", 5432)),
        dbname=os.environ.get("PGDATABASE", "orders"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
    )


def bar_chart(df, x, y, title, filename, horizontal=False):
    """Create a simple bar chart."""
    if df.empty:
        return None
    fig, ax = plt.subplots(figsize=(10, 5))
    if horizontal:
        ax.barh(df[x], df[y])
        ax.invert_yaxis()
    else:
        ax.bar(df[x].astype(str), df[y])
        ax.tick_params(axis="x", rotation=45)
    ax.set_title(title, fontweight="bold")
    fig.tight_layout()
    path = REPORT_DIR / filename
    fig.savefig(path, dpi=100)
    plt.close(fig)
    return path


def generate_report():
    """Generate Report.md with charts and tables."""
    print("Generating report...")
    REPORT_DIR.mkdir(exist_ok=True)

    with get_connection() as conn:
        data = {v: pd.read_sql(f"SELECT * FROM {v}", conn) for views in VIEWS.values() for v in views}

    # Charts
    bar_chart(data["v_daily_metrics"], "date", "total_revenue", "Daily Revenue", "daily_metrics.png")
    bar_chart(data["v_top_customers_by_spend"], "email", "lifetime_spend", "Top Customers", "top_customers.png", horizontal=True)
    bar_chart(data["v_top_skus"], "sku", "revenue", "Top SKUs", "top_skus.png", horizontal=True)

    # Build markdown
    def table(df):
        return df.to_markdown(index=False) if not df.empty else "_No data_"

    md = f"""# Orders Pipeline Report

_Generated: {datetime.now():%Y-%m-%d %H:%M:%S}_

## Analytics

### Daily Metrics
![](reports/daily_metrics.png)

{table(data["v_daily_metrics"])}

### Top Customers
![](reports/top_customers.png)

{table(data["v_top_customers_by_spend"])}

### Top SKUs
![](reports/top_skus.png)

{table(data["v_top_skus"])}

---

## Data Quality

### Rejection Summary
{table(data["v_dq_rejection_summary"])}

### Duplicate Emails
{table(data["v_dq_duplicate_emails"])}

### Orphan Orders
{table(data["v_dq_orphan_orders"])}

### Invalid Order Items
{table(data["v_dq_invalid_order_items"])}

### Invalid Status
{table(data["v_dq_invalid_status"])}
"""

    REPORT_FILE.write_text(md)
    print(f"Report saved to {REPORT_FILE}")


if __name__ == "__main__":
    generate_report()
