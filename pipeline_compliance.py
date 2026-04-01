"""
Pipeline Compliance Notification System

Queries SFDC opportunity data via Databricks, evaluates 12 non-compliance rules
across 8 buckets, and sends formatted Slack notifications.

This script is designed to be run interactively in an environment where the
Databricks MCP and Slack MCP are available (e.g., Cursor IDE). The MCP calls
are represented as structured function calls that the operator executes.

Usage:
    Run this script, then execute the generated SQL via Databricks MCP and
    the Slack messages via Slack MCP. The script prints all SQL and messages
    for review before any action is taken.
"""

import json
import sys
from datetime import datetime, date
from collections import defaultdict

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def load_config(path="compliance_config.json"):
    with open(path, "r") as f:
        return json.load(f)

CONFIG = load_config()

BUCKETS = [
    "Missing Campaign",
    "Missing Product (Sol+)",
    "Missing Next Step (Sol+)",
    "Close Date Passed",
    "Stale Opportunity",
    "Missing Future Activity",
    "Stale Stage Duration",
    "Incorrect Accountant Setup",
]

MM_DIVISIONS_SQL = ", ".join(f"'{d}'" for d in CONFIG["mm_divisions"])
SOL_PLUS_SQL = ", ".join(f"'{s}'" for s in CONFIG["sol_plus_stages"])
CLOSED_STAGES_SQL = ", ".join(f"'{s}'" for s in CONFIG["closed_stages"])

# ---------------------------------------------------------------------------
# SQL Queries
# ---------------------------------------------------------------------------

COMPLIANCE_SQL = f"""
WITH open_opps AS (
    SELECT
        o.id                        AS opp_id,
        o.name                      AS opp_name,
        o.stagename,
        o.closedate,
        o.lastmodifieddate,
        o.laststagechangedate,
        o.nextstep,
        o.campaignid,
        o.hasopportunitylineitem,
        o.ownerid,
        o.createdbyid,
        u.division                  AS owner_division,
        u.managerid                 AS owner_managerid,
        c.type                      AS campaign_type,
        c.name                      AS campaign_name
    FROM ued_salesforce_dwh.sales_opportunity o
    JOIN ued_salesforce_dwh.sales_user u
        ON o.ownerid = u.id
    LEFT JOIN ued_salesforce_dwh.sales_campaign c
        ON o.campaignid = c.id
    WHERE o.isclosed = false
      AND o.isdeleted = false
      AND u.isactive = true
      AND u.division IN ({MM_DIVISIONS_SQL})
),

future_activities AS (
    SELECT DISTINCT whatid AS opp_id
    FROM ued_salesforce_dwh.sales_task
    WHERE isdeleted = false
      AND activitydate >= CURRENT_DATE()
      AND status != 'Completed'
    UNION
    SELECT DISTINCT whatid AS opp_id
    FROM ued_salesforce_dwh.sales_event
    WHERE isdeleted = false
      AND startdatetime >= CURRENT_DATE()
),

channel_managers AS (
    SELECT DISTINCT opportunityid AS opp_id, userid
    FROM ued_salesforce_dwh.sales_opportunityteammember
    WHERE isdeleted = false
      AND teammemberrole = 'Channel Manager'
      AND userid IS NOT NULL
),

accountant_creators AS (
    SELECT id
    FROM ued_salesforce_dwh.sales_user
    WHERE isactive = true
      AND (
          lower(title) LIKE '%accountant%ib%'
          OR lower(title) LIKE '%accountant%ob%'
          OR lower(division) LIKE '%accountant%'
          OR lower(program__c) LIKE '%accountant%'
      )
),

valid_accounts AS (
    SELECT DISTINCT company_id__c AS company_id
    FROM ued_salesforce_dwh.sales_account
    WHERE company_id__c IS NOT NULL
      AND isdeleted = false
)

SELECT
    oo.opp_id,
    oo.opp_name,
    oo.stagename,
    oo.closedate,
    oo.lastmodifieddate,
    oo.laststagechangedate,
    oo.nextstep,
    oo.campaignid,
    oo.hasopportunitylineitem,
    oo.ownerid,
    oo.createdbyid,
    oo.owner_division,
    oo.owner_managerid,
    oo.campaign_type,
    oo.campaign_name,
    CASE WHEN fa.opp_id IS NOT NULL THEN true ELSE false END AS has_future_activity,
    CASE WHEN cm.opp_id IS NOT NULL THEN true ELSE false END AS has_channel_manager,
    CASE WHEN ac.id IS NOT NULL THEN true ELSE false END AS created_by_accountant,

    -- Bucket flags
    CASE WHEN oo.campaignid IS NULL THEN 1 ELSE 0 END
        AS flag_missing_campaign,

    CASE WHEN oo.stagename IN ({SOL_PLUS_SQL})
         AND oo.hasopportunitylineitem = false THEN 1 ELSE 0 END
        AS flag_missing_product,

    CASE WHEN oo.stagename IN ({SOL_PLUS_SQL})
         AND (oo.nextstep IS NULL OR TRIM(oo.nextstep) = '') THEN 1 ELSE 0 END
        AS flag_missing_next_step,

    CASE WHEN oo.closedate < CURRENT_DATE() THEN 1 ELSE 0 END
        AS flag_close_date_passed,

    CASE WHEN DATEDIFF(CURRENT_DATE(), oo.lastmodifieddate) > {CONFIG['stale_opportunity_days']}
         THEN 1 ELSE 0 END
        AS flag_stale_opportunity,

    CASE WHEN fa.opp_id IS NULL THEN 1 ELSE 0 END
        AS flag_missing_future_activity,

    CASE
        WHEN oo.stagename IN ('Prospecting', 'Assess')
             AND oo.laststagechangedate IS NOT NULL
             AND DATEDIFF(CURRENT_DATE(), oo.laststagechangedate) > {CONFIG['stale_stage_early_days']}
             THEN 1
        WHEN oo.stagename IN ('Solution', 'Propose')
             AND oo.laststagechangedate IS NOT NULL
             AND DATEDIFF(CURRENT_DATE(), oo.laststagechangedate) > {CONFIG['stale_stage_late_days']}
             THEN 1
        ELSE 0
    END AS flag_stale_stage_duration,

    CASE
        WHEN lower(oo.campaign_type) = 'accountant referral'
             AND oo.lead_xref_id IS NULL THEN 1
        WHEN ac.id IS NOT NULL
             AND oo.lead_xref_id IS NULL THEN 1
        WHEN oo.lead_xref_id IS NOT NULL
             AND va.company_id IS NULL THEN 1
        WHEN oo.lead_xref_id IS NOT NULL
             AND cm.opp_id IS NULL THEN 1
        ELSE 0
    END AS flag_incorrect_accountant_setup

FROM open_opps oo
LEFT JOIN future_activities fa ON oo.opp_id = fa.opp_id
LEFT JOIN channel_managers cm ON oo.opp_id = cm.opp_id
LEFT JOIN accountant_creators ac ON oo.createdbyid = ac.id
LEFT JOIN valid_accounts va ON oo.lead_xref_id = va.company_id
"""

# The lead_xref_id__c field is the referrer ID on the opportunity.
# We need to reference it properly. Let's fix the SQL to use the correct column.
COMPLIANCE_SQL = COMPLIANCE_SQL.replace("oo.lead_xref_id", "o_ref.lead_xref_id__c")

# Actually, let's rebuild cleanly since the referrer field (lead_xref_id__c)
# is on the opportunity table directly. We need to include it in the open_opps CTE.
COMPLIANCE_SQL = f"""
WITH open_opps AS (
    SELECT
        o.id                        AS opp_id,
        o.name                      AS opp_name,
        o.stagename,
        o.closedate,
        o.lastmodifieddate,
        o.laststagechangedate,
        o.nextstep,
        o.campaignid,
        o.hasopportunitylineitem,
        o.ownerid,
        o.createdbyid,
        o.lead_xref_id__c           AS referrer_id,
        u.division                  AS owner_division,
        u.managerid                 AS owner_managerid,
        c.type                      AS campaign_type,
        c.name                      AS campaign_name
    FROM ued_salesforce_dwh.sales_opportunity o
    JOIN ued_salesforce_dwh.sales_user u
        ON o.ownerid = u.id
    LEFT JOIN ued_salesforce_dwh.sales_campaign c
        ON o.campaignid = c.id
    WHERE o.isclosed = false
      AND o.isdeleted = false
      AND u.isactive = true
      AND u.division IN ({MM_DIVISIONS_SQL})
),

future_activities AS (
    SELECT DISTINCT whatid AS opp_id
    FROM ued_salesforce_dwh.sales_task
    WHERE isdeleted = false
      AND activitydate >= CURRENT_DATE()
      AND status != 'Completed'
    UNION
    SELECT DISTINCT whatid AS opp_id
    FROM ued_salesforce_dwh.sales_event
    WHERE isdeleted = false
      AND startdatetime >= CURRENT_DATE()
),

channel_managers AS (
    SELECT DISTINCT opportunityid AS opp_id, userid
    FROM ued_salesforce_dwh.sales_opportunityteammember
    WHERE isdeleted = false
      AND teammemberrole = 'Channel Manager'
      AND userid IS NOT NULL
),

accountant_creators AS (
    SELECT id
    FROM ued_salesforce_dwh.sales_user
    WHERE isactive = true
      AND (
          lower(title) LIKE '%accountant%ib%'
          OR lower(title) LIKE '%accountant%ob%'
          OR lower(division) LIKE '%accountant%'
          OR lower(program__c) LIKE '%accountant%'
      )
),

valid_accounts AS (
    SELECT DISTINCT company_id__c AS company_id
    FROM ued_salesforce_dwh.sales_account
    WHERE company_id__c IS NOT NULL
      AND isdeleted = false
)

SELECT
    oo.opp_id,
    oo.opp_name,
    oo.stagename,
    oo.closedate,
    oo.lastmodifieddate,
    oo.laststagechangedate,
    oo.nextstep,
    oo.campaignid,
    oo.hasopportunitylineitem,
    oo.ownerid,
    oo.createdbyid,
    oo.referrer_id,
    oo.owner_division,
    oo.owner_managerid,
    oo.campaign_type,
    oo.campaign_name,
    CASE WHEN fa.opp_id IS NOT NULL THEN true ELSE false END AS has_future_activity,
    CASE WHEN cm.opp_id IS NOT NULL THEN true ELSE false END AS has_channel_manager,
    CASE WHEN ac.id IS NOT NULL THEN true ELSE false END AS created_by_accountant,

    CASE WHEN oo.campaignid IS NULL THEN 1 ELSE 0 END
        AS flag_missing_campaign,

    CASE WHEN oo.stagename IN ({SOL_PLUS_SQL})
         AND oo.hasopportunitylineitem = false THEN 1 ELSE 0 END
        AS flag_missing_product,

    CASE WHEN oo.stagename IN ({SOL_PLUS_SQL})
         AND (oo.nextstep IS NULL OR TRIM(oo.nextstep) = '') THEN 1 ELSE 0 END
        AS flag_missing_next_step,

    CASE WHEN oo.closedate < CURRENT_DATE() THEN 1 ELSE 0 END
        AS flag_close_date_passed,

    CASE WHEN DATEDIFF(CURRENT_DATE(), oo.lastmodifieddate) > {CONFIG['stale_opportunity_days']}
         THEN 1 ELSE 0 END
        AS flag_stale_opportunity,

    CASE WHEN fa.opp_id IS NULL THEN 1 ELSE 0 END
        AS flag_missing_future_activity,

    CASE
        WHEN oo.stagename IN ('Prospecting', 'Assess')
             AND oo.laststagechangedate IS NOT NULL
             AND DATEDIFF(CURRENT_DATE(), oo.laststagechangedate) > {CONFIG['stale_stage_early_days']}
             THEN 1
        WHEN oo.stagename IN ('Solution', 'Propose')
             AND oo.laststagechangedate IS NOT NULL
             AND DATEDIFF(CURRENT_DATE(), oo.laststagechangedate) > {CONFIG['stale_stage_late_days']}
             THEN 1
        ELSE 0
    END AS flag_stale_stage_duration,

    CASE
        WHEN lower(oo.campaign_type) = 'accountant referral'
             AND (oo.referrer_id IS NULL OR TRIM(oo.referrer_id) = '') THEN 1
        WHEN ac.id IS NOT NULL
             AND (oo.referrer_id IS NULL OR TRIM(oo.referrer_id) = '') THEN 1
        WHEN oo.referrer_id IS NOT NULL AND TRIM(oo.referrer_id) != ''
             AND va.company_id IS NULL THEN 1
        WHEN oo.referrer_id IS NOT NULL AND TRIM(oo.referrer_id) != ''
             AND cm.opp_id IS NULL THEN 1
        ELSE 0
    END AS flag_incorrect_accountant_setup

FROM open_opps oo
LEFT JOIN future_activities fa ON oo.opp_id = fa.opp_id
LEFT JOIN channel_managers cm ON oo.opp_id = cm.opp_id
LEFT JOIN accountant_creators ac ON oo.createdbyid = ac.id
LEFT JOIN valid_accounts va ON oo.referrer_id = va.company_id
"""

OWNER_NAMES_SQL = """
SELECT DISTINCT
    opty_ownerid,
    opty_ownername,
    opty_owner_div_l3,
    Opty_owner_mgrname
FROM sales_published.sales_e2e_funnel_details
WHERE opty_owner_div_l3 IN ({divisions})
  AND opty_ownerid IS NOT NULL
  AND opty_ownername IS NOT NULL
""".format(divisions=MM_DIVISIONS_SQL)

# Mapping from raw SFDC user IDs to funnel owner IDs requires a bridge.
# The funnel table uses a different ID format. We'll build the name map
# by joining through the opportunity ID instead.
OWNER_NAME_MAP_SQL = f"""
SELECT DISTINCT
    o.ownerid,
    f.opty_ownername,
    f.opty_owner_div_l3,
    f.Opty_owner_mgrname
FROM ued_salesforce_dwh.sales_opportunity o
JOIN sales_published.sales_e2e_funnel_details f
    ON CONCAT('SFDC_SALES.id.', o.id) = f.Opty_ID
JOIN ued_salesforce_dwh.sales_user u
    ON o.ownerid = u.id
WHERE u.isactive = true
  AND u.division IN ({MM_DIVISIONS_SQL})
  AND f.opty_ownername IS NOT NULL
"""

# ---------------------------------------------------------------------------
# Data Processing
# ---------------------------------------------------------------------------

FLAG_COLUMNS = [
    "flag_missing_campaign",
    "flag_missing_product",
    "flag_missing_next_step",
    "flag_close_date_passed",
    "flag_stale_opportunity",
    "flag_missing_future_activity",
    "flag_stale_stage_duration",
    "flag_incorrect_accountant_setup",
]

FLAG_TO_BUCKET = dict(zip(FLAG_COLUMNS, BUCKETS))


def parse_results(columns, rows):
    """Convert Databricks result format to list of dicts."""
    return [dict(zip([c for c in columns], [r[c] for c in columns])) for r in rows]


def aggregate_by_owner(results):
    """Group non-compliance counts by owner ID."""
    owner_buckets = defaultdict(lambda: {b: 0 for b in BUCKETS})
    owner_opp_ids = defaultdict(lambda: {b: [] for b in BUCKETS})
    owner_total = defaultdict(int)

    for row in results:
        owner_id = row["ownerid"]
        owner_total[owner_id] += 1
        for flag_col, bucket in FLAG_TO_BUCKET.items():
            val = row.get(flag_col, 0)
            if str(val) == "1":
                owner_buckets[owner_id][bucket] += 1
                owner_opp_ids[owner_id][bucket].append(row["opp_id"])

    return owner_buckets, owner_opp_ids, owner_total


def aggregate_by_manager(owner_buckets, name_map):
    """Roll up owner-level counts to manager level."""
    manager_team = defaultdict(lambda: defaultdict(lambda: {b: 0 for b in BUCKETS}))

    for owner_id, buckets in owner_buckets.items():
        info = name_map.get(owner_id, {})
        mgr_name = info.get("manager_name", "Unknown Manager")
        owner_name = info.get("owner_name", owner_id)
        for bucket, count in buckets.items():
            manager_team[mgr_name][owner_name][bucket] += count

    return manager_team


def build_name_map(name_rows):
    """Build owner_id -> {owner_name, division, manager_name} mapping."""
    name_map = {}
    for row in name_rows:
        owner_id = row.get("ownerid")
        if owner_id:
            name_map[owner_id] = {
                "owner_name": row.get("opty_ownername", "Unknown"),
                "division": row.get("opty_owner_div_l3", "Unknown"),
                "manager_name": row.get("Opty_owner_mgrname") or "Unknown Manager",
            }
    return name_map


# ---------------------------------------------------------------------------
# Message Formatting
# ---------------------------------------------------------------------------

def format_rep_message(owner_name, bucket_counts, dashboard_url):
    """Build the Slack notification for an individual rep."""
    first_name = owner_name.split()[0] if owner_name else "Team Member"

    total_issues = sum(bucket_counts.values())
    if total_issues == 0:
        return None

    table_rows = []
    for bucket in BUCKETS:
        count = bucket_counts.get(bucket, 0)
        table_rows.append(f"| {bucket:<27} | {count:>9} |")

    table = "\n".join(table_rows)

    msg = (
        f"Hey {first_name} -- quick pipeline hygiene check-in for the week.\n\n"
        f"You currently have *{total_issues} opportunities* that need attention to stay "
        f"compliant with our pipeline standards. Keeping these clean helps us forecast "
        f"more accurately and move deals forward efficiently. Here's a quick summary:\n\n"
        f"```\n"
        f"| {'Compliance Bucket':<27} | {'Open Opps':>9} |\n"
        f"|{'-'*29}|{'-'*11}|\n"
        f"{table}\n"
        f"```\n\n"
        f"Take a few minutes to review and update these here:\n"
        f"{dashboard_url}"
    )
    return msg


def format_manager_message(manager_name, team_data, dashboard_url):
    """Build the Slack notification for a manager/FLM."""
    first_name = manager_name.split()[0] if manager_name else "Manager"

    total_issues = 0
    for owner_buckets in team_data.values():
        total_issues += sum(owner_buckets.values())

    if total_issues == 0:
        return None

    header = f"| {'Rep Name':<30} |"
    divider = f"|{'-'*32}|"
    for bucket in BUCKETS:
        short = bucket.replace(" (Sol+)", "").replace("Incorrect ", "")[:12]
        header += f" {short:>12} |"
        divider += f"{'-'*14}|"

    rows = []
    for rep_name in sorted(team_data.keys()):
        buckets = team_data[rep_name]
        rep_total = sum(buckets.values())
        if rep_total == 0:
            continue
        row = f"| {rep_name:<30} |"
        for bucket in BUCKETS:
            row += f" {buckets.get(bucket, 0):>12} |"
        rows.append(row)

    if not rows:
        return None

    table = "\n".join(rows)

    msg = (
        f"Hey {first_name} -- here's your team's weekly pipeline compliance summary.\n\n"
        f"Your team currently has *{total_issues} total non-compliant opportunities* across "
        f"the buckets below. Encouraging your reps to clean these up will help the team "
        f"forecast more accurately and keep deals moving. Here's the breakdown:\n\n"
        f"```\n"
        f"{header}\n"
        f"{divider}\n"
        f"{table}\n"
        f"```\n\n"
        f"Review your team's opportunities and help them get current here:\n"
        f"{dashboard_url}"
    )
    return msg


# ---------------------------------------------------------------------------
# Validation & Guardrails
# ---------------------------------------------------------------------------

def validate_results(owner_buckets, owner_total, name_map):
    """Run sanity checks before sending any messages."""
    issues = []

    for owner_id, total in owner_total.items():
        non_compliant = sum(owner_buckets[owner_id].values())
        if non_compliant > CONFIG["max_non_compliant_per_user"]:
            name = name_map.get(owner_id, {}).get("owner_name", owner_id)
            issues.append(
                f"WARN: {name} has {non_compliant} non-compliant opps "
                f"(threshold: {CONFIG['max_non_compliant_per_user']})"
            )

    for owner_id in owner_buckets:
        info = name_map.get(owner_id, {})
        div = info.get("division", "UNKNOWN")
        if div not in CONFIG["mm_divisions"] and div != "Unknown":
            issues.append(f"WARN: Owner {owner_id} has unexpected division: {div}")

    return issues


def print_summary(owner_buckets, owner_total, name_map):
    """Print a console summary for review."""
    print("\n" + "=" * 70)
    print("PIPELINE COMPLIANCE SUMMARY")
    print(f"Run Date: {date.today().isoformat()}")
    print(f"Mode: {'TEST' if CONFIG['test_mode'] else 'PRODUCTION'}")
    print(f"Dry Run: {CONFIG['dry_run']}")
    print("=" * 70)

    total_opps = sum(owner_total.values())
    total_non_compliant = sum(
        sum(b.values()) for b in owner_buckets.values()
    )
    print(f"\nTotal open opportunities evaluated: {total_opps}")
    print(f"Total non-compliant flags: {total_non_compliant}")
    print(f"Unique owners: {len(owner_buckets)}")

    agg = {b: 0 for b in BUCKETS}
    for buckets in owner_buckets.values():
        for bucket, count in buckets.items():
            agg[bucket] += count

    print(f"\n{'Bucket':<30} {'Count':>8}")
    print("-" * 40)
    for bucket in BUCKETS:
        print(f"{bucket:<30} {agg[bucket]:>8}")

    print("\n" + "-" * 70)


def print_owner_detail(owner_id, owner_buckets, owner_opp_ids, name_map):
    """Print detailed breakdown for a single owner."""
    info = name_map.get(owner_id, {})
    name = info.get("owner_name", owner_id)
    div = info.get("division", "Unknown")
    mgr = info.get("manager_name", "Unknown")

    print(f"\n  Owner: {name}")
    print(f"  Division: {div}")
    print(f"  Manager: {mgr}")

    buckets = owner_buckets.get(owner_id, {})
    for bucket in BUCKETS:
        count = buckets.get(bucket, 0)
        if count > 0:
            opp_ids = owner_opp_ids[owner_id][bucket][:5]
            ids_str = ", ".join(opp_ids)
            more = f" (+{count - 5} more)" if count > 5 else ""
            print(f"    {bucket}: {count}  [{ids_str}{more}]")


# ---------------------------------------------------------------------------
# Main Orchestration
# ---------------------------------------------------------------------------

def get_sql():
    """Return the compliance SQL query."""
    return COMPLIANCE_SQL


def get_name_map_sql():
    """Return the SQL to build the owner name mapping."""
    return OWNER_NAME_MAP_SQL


def process_compliance_data(compliance_rows, name_rows):
    """
    Process raw query results into structured compliance data.

    Args:
        compliance_rows: list of dicts from the compliance SQL
        name_rows: list of dicts from the name mapping SQL

    Returns:
        dict with all processed data needed for notifications
    """
    name_map = build_name_map(name_rows)
    owner_buckets, owner_opp_ids, owner_total = aggregate_by_owner(compliance_rows)

    issues = validate_results(owner_buckets, owner_total, name_map)
    if issues:
        print("\n*** VALIDATION WARNINGS ***")
        for issue in issues:
            print(f"  {issue}")

    print_summary(owner_buckets, owner_total, name_map)

    if CONFIG["test_mode"]:
        test_rep = CONFIG["test_rep_name"]
        test_mgr = CONFIG["test_manager_name"]

        test_owner_id = None
        for oid, info in name_map.items():
            if info["owner_name"] == test_rep:
                test_owner_id = oid
                break

        if test_owner_id:
            print(f"\n--- Test Rep: {test_rep} ---")
            print_owner_detail(test_owner_id, owner_buckets, owner_opp_ids, name_map)

            rep_msg = format_rep_message(
                test_rep,
                owner_buckets.get(test_owner_id, {b: 0 for b in BUCKETS}),
                CONFIG["rep_dashboard_url"],
            )
        else:
            print(f"\nWARN: Test rep '{test_rep}' not found in results.")
            rep_msg = None

        manager_team = aggregate_by_manager(owner_buckets, name_map)
        mgr_data = manager_team.get(test_mgr)

        if mgr_data:
            print(f"\n--- Test Manager: {test_mgr} ---")
            for rep_name, buckets in mgr_data.items():
                total = sum(buckets.values())
                if total > 0:
                    print(f"  {rep_name}: {total} non-compliant")

            mgr_msg = format_manager_message(
                test_mgr, mgr_data, CONFIG["manager_dashboard_url"]
            )
        else:
            print(f"\nWARN: Test manager '{test_mgr}' not found in results.")
            mgr_msg = None

        return {
            "mode": "test",
            "rep_message": rep_msg,
            "manager_message": mgr_msg,
            "test_rep_name": test_rep,
            "test_manager_name": test_mgr,
            "owner_buckets": owner_buckets,
            "name_map": name_map,
        }

    else:
        manager_team = aggregate_by_manager(owner_buckets, name_map)

        rep_messages = {}
        for owner_id, buckets in owner_buckets.items():
            info = name_map.get(owner_id, {})
            name = info.get("owner_name", "Team Member")
            msg = format_rep_message(name, buckets, CONFIG["rep_dashboard_url"])
            if msg:
                rep_messages[owner_id] = {"name": name, "message": msg}

        mgr_messages = {}
        for mgr_name, team_data in manager_team.items():
            msg = format_manager_message(
                mgr_name, team_data, CONFIG["manager_dashboard_url"]
            )
            if msg:
                mgr_messages[mgr_name] = {"message": msg}

        return {
            "mode": "production",
            "rep_messages": rep_messages,
            "manager_messages": mgr_messages,
            "owner_buckets": owner_buckets,
            "name_map": name_map,
        }


def log_action(action, detail=""):
    """Append to the log file."""
    timestamp = datetime.now().isoformat()
    with open(CONFIG["log_file"], "a") as f:
        f.write(f"[{timestamp}] {action}: {detail}\n")


# ---------------------------------------------------------------------------
# Entry point for interactive use
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("Pipeline Compliance Notification System")
    print("=" * 50)
    print(f"Mode: {'TEST' if CONFIG['test_mode'] else 'PRODUCTION'}")
    print(f"Dry Run: {CONFIG['dry_run']}")
    print()
    print("Step 1: Execute the following SQL via Databricks MCP (execute_sql):")
    print("  - Compliance query (COMPLIANCE_SQL)")
    print("  - Name mapping query (OWNER_NAME_MAP_SQL)")
    print()
    print("Step 2: Pass the results to process_compliance_data()")
    print()
    print("Step 3: Review the output and send messages via Slack MCP")
    print()
    print("To get the SQL queries, call:")
    print("  get_sql()           -> main compliance query")
    print("  get_name_map_sql()  -> owner name mapping query")
