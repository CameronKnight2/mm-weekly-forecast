"""
MM Sales Weekly Forecast — Data Refresh Script

Pulls billed actuals from Databricks (salesfunnel_dg), Big Deals from SFDC
opportunity tables, and plan targets from Google Sheets. Outputs forecast_data.json
for the HTML dashboard.

Designed to be run in the Cursor IDE where Databricks MCP (execute_sql) and
Google Drive MCP (sheets_read) are available. Execute the SQL/MCP calls printed
by this script, then feed results back to generate the JSON.

Usage:
    1. Run this script in Cursor
    2. Execute the generated Databricks SQL via MCP
    3. The script writes forecast_data.json consumed by mm_weekly_forecast.html
"""

import json
from datetime import datetime, date, timedelta

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DIVISION_MAP = {
    "MM Account Managers L3": "KAM",
    "MM Channel Sales L3": "Channel",
    "MM East AE L3": "East",
    "MM East Sr. AE L3": "East",
    "MM West AE L3": "West",
    "MM West Sr. AE L3": "West",
}

MM_DIVISIONS = list(DIVISION_MAP.keys())
MM_DIVISIONS_SQL = ", ".join(f"'{d}'" for d in MM_DIVISIONS)

Q3_START_WEEK = 28
Q3_END_WEEK = 40
Q3_WEEKS_TOTAL = Q3_END_WEEK - Q3_START_WEEK + 1

FORECAST_SHEET_ID = "191zU3tjtH6-zfbGfP_dDSmEDsuuk5EhA_L-8cxs925U"
TARGETS_SHEET_ID = "1OjpFA6kAB2D4UtvgsGM8dty8e8HjsA3IxNaedECnSus"

# ---------------------------------------------------------------------------
# SQL: Billed Actuals (weekly + QTD + MTD)
# ---------------------------------------------------------------------------

ACTUALS_SQL = f"""
WITH raw_actuals AS (
    SELECT
        division_l3,
        metric_name,
        ies_opty_flag,
        week_544,
        week_start_date_544,
        SUM(metric_value_numerator) AS metric_value
    FROM sales_published.salesfunnel_dg
    WHERE metric_name IN ('Total Online Sales ($)', 'Total Units')
      AND division_l3 IN ({MM_DIVISIONS_SQL})
      AND year_544 = 2026
      AND week_544 BETWEEN {Q3_START_WEEK} AND {Q3_END_WEEK}
    GROUP BY division_l3, metric_name, ies_opty_flag, week_544, week_start_date_544
)
SELECT
    division_l3,
    metric_name,
    ies_opty_flag,
    week_544,
    week_start_date_544,
    metric_value
FROM raw_actuals
ORDER BY division_l3, metric_name, ies_opty_flag, week_544
"""

# ---------------------------------------------------------------------------
# SQL: Big Deals (open Sol+ opportunities, incremental > $25K)
# ---------------------------------------------------------------------------

BIG_DEALS_SQL = f"""
SELECT
    a.name                          AS account_name,
    o.name                          AS opportunity_name,
    u.division                      AS division,
    mgr.name                        AS manager_name,
    u.name                          AS opportunity_owner,
    o.closedate                     AS close_date,
    o.stagename                     AS stage,
    o.stage_reason__c               AS stage_reason,
    o.amount                        AS amount,
    o.incremental_revenue__c        AS incremental_revenue,
    DATEDIFF(CURRENT_DATE(), o.createddate) AS age_days,
    o.nextstep                      AS next_step
FROM ued_salesforce_dwh.sales_opportunity o
JOIN ued_salesforce_dwh.sales_user u
    ON o.ownerid = u.id
LEFT JOIN ued_salesforce_dwh.sales_user mgr
    ON u.managerid = mgr.id
LEFT JOIN ued_salesforce_dwh.sales_account a
    ON o.accountid = a.id
WHERE o.isclosed = false
  AND o.isdeleted = false
  AND u.isactive = true
  AND u.division IN ({MM_DIVISIONS_SQL})
  AND o.stagename IN ('Solution', 'Propose', 'Commit')
  AND COALESCE(o.incremental_revenue__c, 0) > 25000
ORDER BY o.incremental_revenue__c DESC
"""

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def map_division(division_l3):
    return DIVISION_MAP.get(division_l3, "Unknown")


def compute_current_week_544():
    """Approximate the current 544-calendar week number.
    Intuit FY26 starts Aug 1 2025 = week 1. Week 28 ~ Feb 2 2026."""
    fy_start = date(2025, 8, 2)
    today = date.today()
    delta_days = (today - fy_start).days
    return max(1, (delta_days // 7) + 1)


def build_forecast_json(actuals_rows, big_deals_rows, targets):
    """Transform raw query results into the forecast_data.json structure."""
    current_week = compute_current_week_544()
    prior_week = current_week - 1
    weeks_remaining = max(1, Q3_END_WEEK - current_week + 1)

    today = date.today()
    month_start_approx = today.replace(day=1)

    divisions = ["Total", "KAM", "East", "West", "Channel"]

    actuals = {
        div: {
            "prior_week": {"ies_revenue": 0, "nonies_revenue": 0, "ies_contracts": 0},
            "current_week": {"ies_revenue": 0, "nonies_revenue": 0, "ies_contracts": 0},
            "mtd": {"ies_revenue": 0, "nonies_revenue": 0, "ies_contracts": 0},
            "qtd": {"ies_revenue": 0, "nonies_revenue": 0, "ies_contracts": 0},
        }
        for div in divisions
    }

    for row in actuals_rows:
        div_l3 = row.get("division_l3", "")
        div = map_division(div_l3)
        if div == "Unknown":
            continue

        metric = row.get("metric_name", "")
        ies_flag = row.get("ies_opty_flag", "")
        week = int(row.get("week_544", 0))
        value = float(row.get("metric_value", 0) or 0)

        is_ies = "IES" in ies_flag and "Non" not in ies_flag
        is_revenue = "Sales" in metric
        is_contracts = "Units" in metric

        if is_revenue:
            key = "ies_revenue" if is_ies else "nonies_revenue"
        elif is_contracts and is_ies:
            key = "ies_contracts"
        else:
            continue

        if week == prior_week:
            actuals[div]["prior_week"][key] += value
            actuals["Total"]["prior_week"][key] += value
        if week == current_week:
            actuals[div]["current_week"][key] += value
            actuals["Total"]["current_week"][key] += value

        if Q3_START_WEEK <= week <= prior_week:
            actuals[div]["qtd"][key] += value
            actuals["Total"]["qtd"][key] += value

        week_start_str = row.get("week_start_date_544", "")
        if week_start_str:
            try:
                ws = datetime.strptime(str(week_start_str)[:10], "%Y-%m-%d").date()
                if ws.month == today.month and ws.year == today.year:
                    actuals[div]["mtd"][key] += value
                    actuals["Total"]["mtd"][key] += value
            except (ValueError, TypeError):
                pass

    big_deals = []
    for row in big_deals_rows:
        div_l3 = row.get("division", "")
        div = map_division(div_l3)
        big_deals.append({
            "account_name": row.get("account_name", ""),
            "opportunity_name": row.get("opportunity_name", ""),
            "division": div,
            "division_l3": div_l3,
            "manager": row.get("manager_name", ""),
            "owner": row.get("opportunity_owner", ""),
            "close_date": str(row.get("close_date", "")),
            "stage": row.get("stage", ""),
            "stage_reason": row.get("stage_reason", ""),
            "amount": float(row.get("amount", 0) or 0),
            "incremental_revenue": float(row.get("incremental_revenue", 0) or 0),
            "age": int(row.get("age_days", 0) or 0),
            "next_step": row.get("next_step", ""),
            "commit_upside": "",
        })

    return {
        "generated_at": datetime.now().isoformat(),
        "current_week_544": current_week,
        "prior_week_544": prior_week,
        "weeks_remaining_q3": weeks_remaining,
        "q3_start_week": Q3_START_WEEK,
        "q3_end_week": Q3_END_WEEK,
        "targets": targets,
        "actuals": actuals,
        "big_deals": big_deals,
    }


def build_targets_from_where_we_stand(wws_data):
    """Parse the Where We Stand tab data into per-division targets."""
    targets = {}
    current_div = None

    for row in wws_data:
        if not row:
            current_div = None
            continue
        clean = [str(c).strip() for c in row]

        if len(clean) >= 3 and clean[2] in ("Total", "KAM", "East", "West", "Channel"):
            if "Metric" not in clean:
                current_div = clean[2]
                targets[current_div] = {}
                continue

        if current_div and len(clean) >= 7:
            metric = clean[4] if len(clean) > 4 else ""
            target_val = clean[6] if len(clean) > 6 else ""
            if metric in ("IES Revenue", "Non-IES Revenue", "Total Revenue", "IES Contracts"):
                targets[current_div][metric] = target_val

    return targets


def build_weekly_plan_from_fy26_target(target_data, current_week):
    """Extract weekly plan values from the FY26 Target sheet.
    The sheet has weeks 1-24+ as columns (header row = Week #, 1, 2, ...).
    We need to find the rows for IES/Non-IES revenue and contracts,
    then extract the value at the column corresponding to current_week."""
    weekly_plan = {}
    for row in target_data:
        if not row or len(row) < 3:
            continue
        label = str(row[1]).strip() if len(row) > 1 else ""
        prefix = str(row[0]).strip() if row[0] else ""

        if "Total Online Sales" in label or "Total Non-IES Sales" in label:
            col_idx = min(current_week + 1, len(row) - 1)
            if col_idx < len(row):
                weekly_plan[f"{prefix}_{label}"] = row[col_idx]

        if "IES Contracts" in label or "Total Units" in label:
            col_idx = min(current_week + 1, len(row) - 1)
            if col_idx < len(row):
                weekly_plan[f"{prefix}_{label}"] = row[col_idx]

    return weekly_plan


# ---------------------------------------------------------------------------
# Main entry point — prints SQL for MCP execution
# ---------------------------------------------------------------------------

def print_queries():
    """Print the SQL queries to be executed via Databricks MCP."""
    print("=" * 70)
    print("STEP 1: Execute this SQL via Databricks MCP (execute_sql)")
    print("        Query: Billed Actuals")
    print("=" * 70)
    print(ACTUALS_SQL)
    print()
    print("=" * 70)
    print("STEP 2: Execute this SQL via Databricks MCP (execute_sql)")
    print("        Query: Big Deals")
    print("=" * 70)
    print(BIG_DEALS_SQL)
    print()
    print("=" * 70)
    print("STEP 3: Read Google Sheet via Drive MCP (sheets_read)")
    print(f"        Spreadsheet: {FORECAST_SHEET_ID}")
    print("        Sheet: Where We Stand")
    print("=" * 70)
    print()
    print("=" * 70)
    print("STEP 4: After collecting results, call build_and_save()")
    print("=" * 70)


def build_and_save(actuals_rows, big_deals_rows, wws_data, fy26_target_data=None):
    """Build the JSON and write to forecast_data.json."""
    targets = build_targets_from_where_we_stand(wws_data)
    current_week = compute_current_week_544()

    if fy26_target_data:
        weekly_plan = build_weekly_plan_from_fy26_target(fy26_target_data, current_week)
        targets["weekly_plan"] = weekly_plan

    data = build_forecast_json(actuals_rows, big_deals_rows, targets)

    with open("forecast_data.json", "w") as f:
        json.dump(data, f, indent=2, default=str)

    print(f"\nWrote forecast_data.json ({len(json.dumps(data))} bytes)")
    print(f"  Current week: {current_week}")
    print(f"  Weeks remaining in Q3: {data['weeks_remaining_q3']}")
    print(f"  Big deals: {len(data['big_deals'])}")
    print(f"  Divisions with targets: {list(targets.keys())}")
    return data


if __name__ == "__main__":
    print_queries()
