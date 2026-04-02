import json
import sys

DIVISION_MAP = {
    'MM Account Managers L3': 'KAM',
    'MM Channel Sales L3': 'Channel',
    'MM East AE L3': 'East',
    'MM East Sr. AE L3': 'East',
    'MM West AE L3': 'West',
    'MM West Sr. AE L3': 'West',
}

raw_path = sys.argv[1]
forecast_path = sys.argv[2]

with open(raw_path) as f:
    raw = json.load(f)

rows = raw['result']['rows']

big_deals = []
for r in rows:
    division = DIVISION_MAP.get(r.get('division', ''), r.get('division', ''))
    amt = r.get('amount', 0)
    inc = r.get('incremental_revenue', 0)
    try:
        amt = float(amt) if amt else 0
    except (ValueError, TypeError):
        amt = 0
    try:
        inc = float(inc) if inc else 0
    except (ValueError, TypeError):
        inc = 0

    acct = r.get('account_name', '') or ''
    if acct.startswith('KEY_IDPS'):
        acct = ''
    mgr = r.get('manager_name', '') or ''
    if mgr.startswith('KEY_IDPS'):
        mgr = ''
    owner = r.get('opportunity_owner', '') or r.get('owner_name', '') or ''
    if owner.startswith('KEY_IDPS'):
        owner = ''

    close = str(r.get('close_date', ''))[:10]

    opp_id = r.get('opportunity_id', '') or ''

    big_deals.append({
        'opportunity_id': opp_id,
        'account_name': acct,
        'opportunity_name': r.get('opportunity_name', ''),
        'division': division,
        'manager': mgr,
        'owner': owner,
        'close_date': close,
        'stage': r.get('stage', ''),
        'stage_reason': r.get('stage_reason', '') or '',
        'amount': amt,
        'incremental_revenue': inc,
        'commit_upside': '',
        'age': int(r.get('age_days', 0) or 0),
        'next_step': r.get('next_step', '') or ''
    })

with open(forecast_path) as f:
    forecast = json.load(f)

forecast['big_deals'] = big_deals

with open(forecast_path, 'w') as f:
    json.dump(forecast, f, indent=2)

by_div = {}
for d in big_deals:
    by_div[d['division']] = by_div.get(d['division'], 0) + 1

print(f"Loaded {len(big_deals)} big deals into {forecast_path}")
for div, count in sorted(by_div.items()):
    print(f"  {div}: {count}")
