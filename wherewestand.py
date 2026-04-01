#!/usr/bin/env python3
"""
Weekly #WhereWeStand Update Generator

Computes QTD actuals vs Q3 targets for MM divisions and formats
a Slack-ready message. Designed to be run by the Cursor agent each
Monday, which feeds in fresh Databricks actuals and posts to Slack.

Usage:
    python wherewestand.py [--week N] [--actuals-json FILE]

If --actuals-json is omitted the script uses the embedded actuals
(last refreshed data).  --week overrides the auto-detected week.
"""

import argparse
import json
import sys
from datetime import date, timedelta
from typing import Dict, Tuple

# ── Q3 Targets from Sheet17 ────────────────────────────────────────
# Source: "Revenue and Contract Targets" Google Sheet, Sheet17
# Column P = Q3 Total Target (weeks 28-40)
# Keys: (division_label, metric_label)

Q3_TARGETS: Dict[Tuple[str, str], float] = {
    # ── Total (all divisions) ──
    ("MM Overall", "IES Online Sales $"):      21_763_374,
    ("MM Overall", "Non-IES Online Sales $"):   5_033_302,
    ("MM Overall", "Total Online Sales $"):    26_796_676,
    ("MM Overall", "IES Contracts"):                2_190,

    # ── KAM  (= MM Account Managers L3) ──
    ("KAM", "IES Online Sales $"):              6_735_501,
    ("KAM", "Non-IES Online Sales $"):          1_296_048,
    ("KAM", "Total Online Sales $"):            8_031_549,
    ("KAM", "IES Contracts"):                        577,

    # ── Channel (= MM Channel Sales L3) ──
    ("Channel", "IES Online Sales $"):          6_853_247,
    ("Channel", "Non-IES Online Sales $"):      1_477_521,
    ("Channel", "Total Online Sales $"):        8_330_768,
    ("Channel", "IES Contracts"):                    787,

    # ── East (= MM East AE L3 + MM East Sr. AE L3) ──
    ("East", "IES Online Sales $"):             4_541_276,
    ("East", "Non-IES Online Sales $"):         1_238_604,
    ("East", "Total Online Sales $"):           5_779_880,
    ("East", "IES Contracts"):                       455,

    # ── West (= MM West AE L3 + MM West Sr. AE L3) ──
    ("West", "IES Online Sales $"):             3_633_350,
    ("West", "Non-IES Online Sales $"):         1_021_126,
    ("West", "Total Online Sales $"):           4_654_476,
    ("West", "IES Contracts"):                       368,
}

# Weekly targets by week number (28-40) for pacing context
WEEKLY_TARGETS: Dict[Tuple[str, str], Dict[int, float]] = {
    ("MM Overall", "IES Online Sales $"):      {28:1461609,29:1611679,30:1120248,31:1534527,32:1633868,33:1596878,34:1553546,35:1626467,36:1734259,37:1730031,38:1810350,39:2074558,40:2275356},
    ("MM Overall", "Non-IES Online Sales $"):  {28:421190,29:416752,30:361228,31:422890,32:401781,33:405553,34:402926,35:392692,36:377990,37:367089,38:365881,39:351586,40:345742},
    ("MM Overall", "Total Online Sales $"):    {28:1882799,29:2028431,30:1481476,31:1957416,32:2035649,33:2002431,34:1956472,35:2019159,36:2112248,37:2097121,38:2176232,39:2426144,40:2621098},
    ("MM Overall", "IES Contracts"):           {28:134,29:148,30:103,31:143,32:163,33:161,34:157,35:166,36:174,37:176,38:193,39:224,40:248},
    ("KAM", "IES Online Sales $"):             {28:452348,29:498793,30:346702,31:474916,32:505661,33:494214,34:480803,35:503372,36:536734,37:535425,38:560283,39:642053,40:704197},
    ("KAM", "Non-IES Online Sales $"):         {28:105371,29:105958,30:88100,31:106859,32:105096,33:105082,34:103428,35:101790,36:98697,37:96225,38:95845,39:92579,40:91018},
    ("KAM", "Total Online Sales $"):           {28:557719,29:604751,30:434802,31:581775,32:610757,33:599296,34:584231,35:605162,36:635430,37:631651,38:656128,39:734632,40:795215},
    ("KAM", "IES Contracts"):                  {28:35,29:40,30:21,31:38,32:43,33:43,34:43,35:44,36:47,37:46,38:52,39:58,40:67},
    ("Channel", "IES Online Sales $"):         {28:460269,29:507525,30:352770,31:483226,32:514507,33:502857,34:489210,35:512171,36:546108,37:544776,38:570068,39:653266,40:716496},
    ("Channel", "Non-IES Online Sales $"):     {28:117929,29:119830,30:96935,31:120372,32:120980,33:120262,34:117680,35:116523,36:113489,37:110911,38:110428,39:107001,40:105181},
    ("Channel", "Total Online Sales $"):       {28:578198,29:627355,30:449705,31:603598,32:635486,33:623119,34:606890,35:628694,36:659597,37:655687,38:680496,39:760266,40:821677},
    ("Channel", "IES Contracts"):              {28:49,29:50,30:53,31:51,32:59,33:56,34:52,35:59,36:60,37:63,38:68,39:82,40:85},
    ("East", "IES Online Sales $"):            {28:304672,29:335956,30:233518,31:319874,32:340584,33:333386,34:324341,35:339565,36:362073,37:361190,38:377959,39:433119,40:475040},
    ("East", "Non-IES Online Sales $"):        {28:108155,29:104488,30:96137,31:107010,32:96351,33:98880,34:99688,35:95684,36:91032,37:87849,38:87654,39:83516,40:82161},
    ("East", "Total Online Sales $"):          {28:412827,29:440444,30:329654,31:426884,32:436935,33:432266,34:424029,35:435249,36:453105,37:449039,38:465613,39:516635,40:557202},
    ("East", "IES Contracts"):                 {28:28,29:32,30:17,31:30,32:34,33:34,34:34,35:34,36:37,37:36,38:41,39:46,40:53},
    ("West", "IES Online Sales $"):            {28:244319,29:269405,30:187259,31:256510,32:273116,33:266422,34:259193,35:271359,36:289345,37:288640,38:302040,39:346121,40:379622},
    ("West", "Non-IES Online Sales $"):        {28:89736,29:86476,30:80056,31:88649,32:79354,33:81329,34:82129,35:78695,36:74771,37:72104,38:71954,39:68490,40:67382},
    ("West", "Total Online Sales $"):          {28:334055,29:355881,30:267316,31:345159,32:352470,33:347751,34:341322,35:350054,36:364116,37:360744,38:373994,39:414611,40:447003},
    ("West", "IES Contracts"):                 {28:22,29:26,30:13,31:24,32:27,33:27,34:27,35:28,36:30,37:29,38:33,39:38,40:43},
}

# Databricks division_l3 → display-label mapping
DIVISION_MAP = {
    "MM Account Managers L3": "KAM",
    "MM Channel Sales L3":   "Channel",
    "MM East AE L3":         "East",
    "MM East Sr. AE L3":     "East",
    "MM West AE L3":         "West",
    "MM West Sr. AE L3":     "West",
}

METRIC_MAP = {
    ("Total Online Sales ($)", "IES Opportunity"):     "IES Online Sales $",
    ("Total Online Sales ($)", "Non-IES Opportunity"): "Non-IES Online Sales $",
    ("Total Units",           "IES Opportunity"):      "IES Contracts",
}

DIVISIONS_ORDERED = ["MM Overall", "KAM", "Channel", "East", "West"]
METRICS_ORDERED = [
    "IES Online Sales $",
    "Non-IES Online Sales $",
    "Total Online Sales $",
    "IES Contracts",
]

Q3_START_WEEK = 28
Q3_END_WEEK = 40

# FY26 week-544 to calendar date anchor: week 28 ends 2026-02-07
WEEK_28_END = date(2026, 2, 7)


def current_fiscal_week() -> int:
    """Estimate the current fiscal week based on today's date."""
    today = date.today()
    delta_weeks = (today - WEEK_28_END).days // 7
    return 28 + delta_weeks + (1 if (today - WEEK_28_END).days % 7 > 0 else 0)


def parse_actuals(rows: list) -> Dict[Tuple[str, str], float]:
    """Aggregate Databricks rows into {(division_label, metric_label): value}."""
    actuals: Dict[Tuple[str, str], float] = {}
    for row in rows:
        div_l3 = row["division_l3"]
        ies_flag = row["ies_opty_flag"]
        metric_db = row["metric_name"]
        value = float(row["metric_value"])

        div_label = DIVISION_MAP.get(div_l3)
        if div_label is None:
            continue

        metric_key = (metric_db, ies_flag)
        metric_label = METRIC_MAP.get(metric_key)
        if metric_label is None:
            continue

        key = (div_label, metric_label)
        actuals[key] = actuals.get(key, 0.0) + value

    for metric in ["IES Online Sales $", "Non-IES Online Sales $", "IES Contracts"]:
        total = sum(actuals.get((d, metric), 0.0) for d in ["KAM", "Channel", "East", "West"])
        actuals[("MM Overall", metric)] = total

    for div in DIVISIONS_ORDERED:
        ies = actuals.get((div, "IES Online Sales $"), 0.0)
        non_ies = actuals.get((div, "Non-IES Online Sales $"), 0.0)
        actuals[(div, "Total Online Sales $")] = ies + non_ies

    return actuals


def fmt_dollar(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v/1_000:,.0f}K"
    return f"${v:,.0f}"


def fmt_units(v: float) -> str:
    return f"{int(round(v)):,}"


def fmt_val(metric: str, v: float) -> str:
    if "Contracts" in metric:
        return fmt_units(v)
    return fmt_dollar(v)


def pct_complete(actual: float, target: float) -> str:
    if target == 0:
        return "N/A"
    return f"{actual / target * 100:.0f}%"


def build_division_block(div: str, actuals: Dict[Tuple[str, str], float],
                         weeks_left: int) -> list:
    """Build Slack-formatted lines for one division."""
    lines = []
    for metric in METRICS_ORDERED:
        key = (div, metric)
        actual = actuals.get(key, 0.0)
        target = Q3_TARGETS.get(key, 0.0)
        remaining = target - actual
        pace = remaining / weeks_left if weeks_left > 0 else 0.0
        pct = pct_complete(actual, target)

        if "IES" in metric and "Non" not in metric and "Contracts" not in metric:
            label = "IES Revenue"
        elif "Non-IES" in metric:
            label = "Non-IES Revenue"
        elif "Total" in metric and "Sales" in metric:
            label = "Total Revenue"
        else:
            label = metric

        lines.append(
            f"  *{label}:*  {fmt_val(metric, actual)} of {fmt_val(metric, target)}  "
            f"({pct})  |  {fmt_val(metric, remaining)} left  ~{fmt_val(metric, pace)}/wk to hit"
        )
    return lines


def build_message(actuals: Dict[Tuple[str, str], float], heading_week: int) -> str:
    weeks_left = Q3_END_WEEK - heading_week + 1
    last_data_week = heading_week - 1

    lines = []
    lines.append(f":bar_chart:  *#WhereWeStand — Heading into Week {heading_week}  (FY26 Q3)*")
    lines.append("")
    lines.append(
        f"Where each group stands against Q3 targets through Week {last_data_week}, "
        f"and what's needed each of the remaining *{weeks_left} weeks* to finish strong."
    )

    # MM Overall first, with separator
    lines.append("")
    lines.append("——————————————————————————————")
    lines.append(":large_blue_circle:  *MM Overall*")
    lines.append("——————————————————————————————")
    lines.extend(build_division_block("MM Overall", actuals, weeks_left))

    # Individual divisions
    for div in ["KAM", "Channel", "East", "West"]:
        lines.append("")
        lines.append(f":small_blue_diamond:  *{div}*")
        lines.extend(build_division_block(div, actuals, weeks_left))

    lines.append("")
    lines.append(f"_Data through Wk{last_data_week} · Targets from FY26 Q3 plan_")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Generate #WhereWeStand Slack update")
    parser.add_argument("--week", type=int, default=None,
                        help="Week we are heading into (default: auto-detect)")
    parser.add_argument("--actuals-json", type=str, default=None,
                        help="Path to JSON file with Databricks query rows")
    args = parser.parse_args()

    heading_week = args.week or current_fiscal_week()

    if args.actuals_json:
        with open(args.actuals_json) as f:
            rows = json.load(f)
    else:
        rows = json.loads(sys.stdin.read()) if not sys.stdin.isatty() else None

    if rows is None:
        print("ERROR: Provide actuals via --actuals-json or stdin.", file=sys.stderr)
        sys.exit(1)

    actuals = parse_actuals(rows)
    message = build_message(actuals, heading_week)
    print(message)


if __name__ == "__main__":
    main()
