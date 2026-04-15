"""
LedgerSync — Matching Engine
Reconciles bank transactions against ledger entries using 4 passes:

  Pass 1  — Exact / high-confidence 1-to-1
  Pass 2  — Near-match 1-to-1
  Pass 3  — Composite 1-to-N  (one bank entry = sum of N ledger entries)
  Pass 4  — Composite N-to-1  (N bank entries = one ledger entry)

Additional features:
  • Multi-currency: amounts normalised to base currency before comparison
  • Account-level matching: account numbers used as a confidence signal
  • Separate debit/credit column support (pre-processed in app before calling here)
  • Absolute + percentage amount tolerance
"""

import re
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from itertools import combinations
from typing import Dict


# ─── Date parsing ─────────────────────────────────────────────────────────────

def _parse_dates(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, format='ISO8601', errors='coerce')
    if parsed.isna().mean() > 0.5 and series.notna().any():
        parsed = pd.to_datetime(series, format='mixed', dayfirst=True, errors='coerce')
    return parsed


# ─── Config ───────────────────────────────────────────────────────────────────

@dataclass
class MatchConfig:
    """Configuration for matching tolerances and feature flags."""

    # 1-to-1 matching
    date_tolerance_days: int   = 3
    amount_tolerance_pct: float = 0.0    # fraction, e.g. 0.01 = 1 %
    amount_tolerance_abs: float = 0.01   # absolute floor (handles rounding)
    reference_weight: float    = 0.40
    amount_weight: float       = 0.35
    date_weight: float         = 0.25

    # Account matching (auto-enabled when account column present)
    account_weight: float = 0.0

    # Currency
    base_currency: str = 'INR'
    fx_rates: Dict[str, float] = field(default_factory=dict)  # {'USD': 84.5, …}

    # Composite matching
    composite_match: bool        = True
    composite_max_items: int     = 2    # max items in one composite group
    composite_date_slack: int    = 14   # days window for composite candidates


# ─── Currency helpers ─────────────────────────────────────────────────────────

def _to_base(amount: float, currency, config: MatchConfig) -> float:
    """Convert amount to base currency using config.fx_rates."""
    if pd.isna(amount):
        return float('nan')
    if not currency or pd.isna(currency) or not config.fx_rates:
        return float(amount)
    ccy  = str(currency).strip().upper()
    base = config.base_currency.strip().upper()
    if ccy == base:
        return float(amount)
    rate = config.fx_rates.get(ccy)
    if rate and rate > 0:
        return float(amount) * float(rate)
    return float(amount)   # unknown currency → use as-is


# ─── FX rate extraction from descriptions ─────────────────────────────────────

# Matches: "@ 83.25", "@83.25", "RATE 83.50", "RATE:91.20", "EXCH 84.0", "CONV 107.8"
_FX_RATE_PATTERNS = [
    re.compile(r'@\s*(\d{1,8}(?:\.\d{1,8})?)', re.IGNORECASE),
    re.compile(
        r'(?:RATE|EXCH(?:ANGE)?|CONV(?:ERSION)?)\s*[:\s]\s*(\d{1,8}(?:\.\d{1,8})?)',
        re.IGNORECASE,
    ),
]


def extract_fx_rate(description: str) -> 'float | None':
    """
    Extract an FX conversion rate embedded in a transaction description.

    Handles patterns like:
        "USD 100 @ 83.25"    → 83.25
        "RATE 83.50"          → 83.50
        "EXCH RATE:91.20"    → 91.20
        "CONV 107.80"         → 107.80

    Returns the rate as float, or None if not found / implausible.
    """
    if not description or pd.isna(description):
        return None
    for pat in _FX_RATE_PATTERNS:
        m = pat.search(str(description))
        if m:
            try:
                rate = float(m.group(1))
                if 0.000001 < rate < 1_000_000:   # sanity: skip values that look like amounts
                    return rate
            except ValueError:
                pass
    return None


def normalize_amount(amount: float, currency, description: str,
                     config: 'MatchConfig') -> float:
    """
    Convert amount to base currency with description-aware FX rate extraction.

    Lookup priority:
        1. Rate embedded in description  (e.g. "USD 100 @ 83.25")
        2. config.fx_rates[currency]
        3. Amount as-is (unknown currency — no conversion)
    """
    if pd.isna(amount):
        return float('nan')
    if not currency or pd.isna(currency):
        return float(amount)
    ccy  = str(currency).strip().upper()
    base = config.base_currency.strip().upper()
    if ccy == base:
        return float(amount)

    desc_rate = extract_fx_rate(str(description) if description else '')
    if desc_rate and desc_rate > 0:
        return float(amount) * desc_rate

    rate = config.fx_rates.get(ccy)
    if rate and rate > 0:
        return float(amount) * float(rate)

    return float(amount)   # unknown rate — use as-is


def _enrich_amounts(df: pd.DataFrame, config: 'MatchConfig') -> pd.DataFrame:
    """
    Pre-compute 'amount_base' (base-currency amount) for every row.

    Vectorized: config-rate multiplication is applied column-wise (one multiply
    per currency group).  Description-regex is applied only on the 'description'
    column via Series.map — faster than df.apply(axis=1) because no row-dict is
    constructed; runs regex per cell on a single string, not a full row object.
    """
    df    = df.copy()
    base  = config.base_currency.strip().upper()
    amts  = df['amount'].astype(float)

    # Default: amount is already in base currency
    amount_base = amts.copy()

    # Bulk-multiply by config FX rates (vectorized per-currency group)
    if config.fx_rates and 'currency' in df.columns:
        ccy_col      = df['currency'].fillna('').str.upper()
        mapped_rates = ccy_col.map(config.fx_rates)          # NaN for unknown/base
        fx_mask = (ccy_col != base) & mapped_rates.notna() & amts.notna()
        if fx_mask.any():
            amount_base[fx_mask] = amts[fx_mask] * mapped_rates[fx_mask]

    # Override rows where description contains an embedded FX rate
    if 'description' in df.columns:
        desc_rates = df['description'].map(extract_fx_rate)  # single-column map
        desc_mask  = desc_rates.notna() & amts.notna()
        if desc_mask.any():
            amount_base[desc_mask] = amts[desc_mask] * desc_rates[desc_mask]

    df['amount_base'] = amount_base
    return df


# ─── Scoring helpers ──────────────────────────────────────────────────────────

def _ref_similarity(ref1: str, ref2: str) -> float:
    """1.0 = exact, 0.5 = substring, 0.0 = no match."""
    if ref1 == ref2:
        return 1.0
    if ref1 in ref2 or ref2 in ref1:
        return 0.5
    return 0.0


def _date_score(date1, date2, tolerance_days: int) -> float:
    """1.0 = same day, decays linearly to 0.0 at tolerance boundary."""
    if pd.isna(date1) or pd.isna(date2):
        return 0.0
    diff = abs((date1 - date2).days)
    if diff == 0:
        return 1.0
    if diff <= tolerance_days:
        return 1.0 - (diff / (tolerance_days + 1))
    return 0.0


def _account_score(acc1, acc2) -> float:
    """
    1.0  = exact match
    0.7  = last-4-digits match (partial account number)
    0.5  = one or both unknown (neutral — don't penalise)
    0.0  = different accounts (block match when account_weight > 0)
    """
    if acc1 is None or acc2 is None:
        return 0.5
    a1 = str(acc1).strip().upper()
    a2 = str(acc2).strip().upper()
    if a1 in ('NONE', 'NAN', '') or a2 in ('NONE', 'NAN', ''):
        return 0.5
    if a1 == a2:
        return 1.0
    if len(a1) >= 4 and len(a2) >= 4 and a1[-4:] == a2[-4:]:
        return 0.7
    return 0.0


def _amount_ok(b_amt, b_ccy, l_amt, l_ccy, config: MatchConfig) -> bool:
    """Currency-aware amount equality check."""
    if pd.isna(b_amt) or pd.isna(l_amt):
        return False
    b = _to_base(b_amt, b_ccy, config)
    l = _to_base(l_amt, l_ccy, config)
    if pd.isna(b) or pd.isna(l):
        return False
    diff = abs(b - l)
    if diff <= config.amount_tolerance_abs:
        return True
    if config.amount_tolerance_pct > 0:
        return diff <= abs(b) * config.amount_tolerance_pct
    return False


def _confidence(b_row: pd.Series, l_row: pd.Series,
                config: MatchConfig, has_account: bool) -> float:
    """Weighted confidence score (0–1). Returns 0 if accounts actively conflict."""
    ref_s  = _ref_similarity(b_row['reference'], l_row['reference'])
    date_s = _date_score(b_row['date'], l_row['date'], config.date_tolerance_days)

    if has_account:
        acct_s = _account_score(b_row.get('account'), l_row.get('account'))
        if acct_s == 0.0:       # known accounts, but they don't match → veto
            return 0.0
        w = (config.reference_weight + config.amount_weight
             + config.date_weight + config.account_weight)
        return (config.reference_weight * ref_s
                + config.amount_weight  * 1.0
                + config.date_weight    * date_s
                + config.account_weight * acct_s) / max(w, 1e-9)

    w = config.reference_weight + config.amount_weight + config.date_weight
    return (config.reference_weight * ref_s
            + config.amount_weight  * 1.0
            + config.date_weight    * date_s) / max(w, 1e-9)


# ─── Standardise ──────────────────────────────────────────────────────────────

def standardize_bank(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    col_map = {
        # date
        'txn_date': 'date', 'transaction_date': 'date', 'value_date': 'date',
        'tran_date': 'date', 'trade_date': 'date',
        # description
        'narration': 'description', 'particulars': 'description',
        'remarks': 'description', 'narrative': 'description', 'memo': 'description',
        # reference
        'reference': 'reference', 'ref_no': 'reference', 'utr': 'reference',
        'txn_id': 'reference', 'trans_id': 'reference',
        'transaction_id': 'reference', 'cheque': 'reference', 'chq': 'reference',
        # amount
        'amount': 'amount', 'debit': 'amount', 'withdrawal': 'amount',
        'net_amount': 'amount',
        # currency
        'currency': 'currency', 'ccy': 'currency', 'curr': 'currency',
        # account
        'account': 'account', 'account_no': 'account', 'acc_no': 'account',
        'account_number': 'account', 'bank_account': 'account', 'acct': 'account',
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    df['date']   = _parse_dates(df['date'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['reference'] = (df.get('reference',
                               pd.Series([''] * len(df), index=df.index))
                       .astype(str).str.strip().str.upper())
    df['description'] = (df.get('description',
                                 pd.Series([''] * len(df), index=df.index))
                         .astype(str).str.strip().str.upper())
    df['currency'] = (df['currency'].astype(str).str.strip().str.upper()
                      if 'currency' in df.columns else pd.Series([None] * len(df), index=df.index))
    df['account']  = (df['account'].astype(str).str.strip().str.upper()
                      if 'account'  in df.columns else pd.Series([None] * len(df), index=df.index))
    df['source']         = 'BANK'
    df['original_index'] = df.index
    return df


def standardize_ledger(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    col_map = {
        # date
        'entry_date': 'date', 'posting_date': 'date', 'doc_date': 'date',
        'tran_date': 'date', 'transaction_date': 'date',
        # reference
        'voucher_no': 'reference', 'document_no': 'reference', 'ref': 'reference',
        'doc_no': 'reference', 'voucher': 'reference',
        # description
        'party_name': 'description', 'vendor': 'description', 'payee': 'description',
        'narration': 'description', 'particulars': 'description', 'memo': 'description',
        # amount
        'amount': 'amount', 'net_amount': 'amount',
        # currency
        'currency': 'currency', 'ccy': 'currency', 'curr': 'currency',
        # account
        'account': 'account', 'account_no': 'account', 'acc_no': 'account',
        'gl_account': 'account', 'account_number': 'account',
        'ledger_account': 'account', 'acct': 'account',
    }
    safe = {k: v for k, v in col_map.items()
            if k in df.columns and v not in df.columns}
    df = df.rename(columns=safe)

    df['date']   = _parse_dates(df['date'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['reference'] = (df.get('reference',
                               pd.Series([''] * len(df), index=df.index))
                       .astype(str).str.strip().str.upper())
    df['description'] = (df.get('description',
                                 pd.Series([''] * len(df), index=df.index))
                         .astype(str).str.strip().str.upper())
    df['currency'] = (df['currency'].astype(str).str.strip().str.upper()
                      if 'currency' in df.columns else pd.Series([None] * len(df), index=df.index))
    df['account']  = (df['account'].astype(str).str.strip().str.upper()
                      if 'account'  in df.columns else pd.Series([None] * len(df), index=df.index))
    df['source']         = 'LEDGER'
    df['original_index'] = df.index
    return df


# ─── Row builders ─────────────────────────────────────────────────────────────

_MATCH_COLS = [
    'bank_date', 'bank_amount', 'bank_reference', 'bank_description',
    'ledger_date', 'ledger_amount', 'ledger_reference', 'ledger_description',
    'confidence', 'match_type',
    'bank_currency', 'ledger_currency',
    'bank_account', 'ledger_account',
]


def _build_match_row(b, l, conf, mtype):
    return [
        b['date'],   b['amount'],   b['reference'],   b['description'],
        l['date'],   l['amount'],   l['reference'],   l['description'],
        round(conf, 2), mtype,
        b.get('currency'), l.get('currency'),
        b.get('account'),  l.get('account'),
    ]


def _build_composite_1_to_n(b_row, combo):
    """One bank → N ledger items."""
    l_refs  = '; '.join(str(r['reference'])        for _, r in combo)
    l_descs = '; '.join(str(r['description'])[:25] for _, r in combo)
    l_total = sum(r['amount'] for _, r in combo)
    l_dates = '; '.join(
        r['date'].strftime('%d-%b') if not pd.isna(r['date']) else '?'
        for _, r in combo
    )
    return [
        b_row['date'], b_row['amount'], b_row['reference'], b_row['description'],
        l_dates, round(l_total, 2), l_refs, l_descs,
        0.75, f'COMPOSITE_1_TO_{len(combo)}',
        b_row.get('currency'), None,
        b_row.get('account'),  None,
    ]


def _build_composite_n_to_1(combo, l_row):
    """N bank items → one ledger."""
    b_refs  = '; '.join(str(r['reference'])        for _, r in combo)
    b_descs = '; '.join(str(r['description'])[:25] for _, r in combo)
    b_total = sum(r['amount'] for _, r in combo)
    b_dates = '; '.join(
        r['date'].strftime('%d-%b') if not pd.isna(r['date']) else '?'
        for _, r in combo
    )
    return [
        b_dates, round(b_total, 2), b_refs, b_descs,
        l_row['date'], l_row['amount'], l_row['reference'], l_row['description'],
        0.75, f'COMPOSITE_{len(combo)}_TO_1',
        None, l_row.get('currency'),
        None, l_row.get('account'),
    ]


# ─── Composite candidate search ───────────────────────────────────────────────

def _subset_sum_search(target: float, candidates: list,
                        max_items: int, tol: float):
    """
    Search for a subset of candidates whose amounts sum to `target` within `tol`.
    candidates: list of (idx, row)
    Returns the matching subset list, or None.
    Caps search at max_items and at 40 candidates (combinatorial safety).
    """
    if len(candidates) < 2:
        return None
    cands = candidates[:40]   # safety limit before combinatorics explode
    for size in range(2, min(max_items + 1, len(cands) + 1)):
        for combo in combinations(cands, size):
            # Use pre-computed base amount when available, fall back to raw amount
            total = sum(
                float(r.get('amount_base', r['amount']))
                for _, r in combo
            )
            if abs(total - target) <= tol:
                return list(combo)
    return None


def _composite_candidates_1_to_n(b_row, unmatched_l: pd.DataFrame,
                                   config: MatchConfig):
    """
    Pre-filter ledger candidates for a 1-to-N search.
    Vectorized: boolean masks on columns instead of row-by-row iterrows.
    iterrows is only called on the small filtered subset (≤40 rows).
    """
    target = float(b_row.get('amount_base', _to_base(b_row['amount'], b_row.get('currency'), config)))
    if pd.isna(target) or unmatched_l.empty:
        return []
    tol = max(config.amount_tolerance_abs,
              abs(target) * max(config.amount_tolerance_pct, 0.001))

    col  = unmatched_l['amount_base']
    mask = col.notna() & (col * target >= 0) & (col.abs() < abs(target) + tol)

    if not pd.isna(b_row['date']) and 'date' in unmatched_l.columns:
        date_diff = (unmatched_l['date'] - b_row['date']).abs()
        mask &= date_diff.isna() | (date_diff <= pd.Timedelta(days=config.composite_date_slack))

    subset = unmatched_l[mask].head(40)   # combinatorial safety cap
    return list(subset.iterrows())


def _composite_candidates_n_to_1(l_row, unmatched_b: pd.DataFrame,
                                   config: MatchConfig):
    """
    Pre-filter bank candidates for an N-to-1 search.
    Vectorized: same approach as _composite_candidates_1_to_n.
    """
    target = float(l_row.get('amount_base', _to_base(l_row['amount'], l_row.get('currency'), config)))
    if pd.isna(target) or unmatched_b.empty:
        return []
    tol = max(config.amount_tolerance_abs,
              abs(target) * max(config.amount_tolerance_pct, 0.001))

    col  = unmatched_b['amount_base']
    mask = col.notna() & (col * target >= 0) & (col.abs() < abs(target) + tol)

    if not pd.isna(l_row['date']) and 'date' in unmatched_b.columns:
        date_diff = (unmatched_b['date'] - l_row['date']).abs()
        mask &= date_diff.isna() | (date_diff <= pd.Timedelta(days=config.composite_date_slack))

    subset = unmatched_b[mask].head(40)
    return list(subset.iterrows())


# ─── Main reconcile ───────────────────────────────────────────────────────────

def reconcile(bank_df: pd.DataFrame, ledger_df: pd.DataFrame,
              config: MatchConfig = None) -> dict:
    """
    4-pass reconciliation engine.

    Returns
    -------
    {
        'matched'          : DataFrame   – Pass 1 exact matches
        'near_matched'     : DataFrame   – Pass 2 near matches
        'composite'        : DataFrame   – Pass 3 + 4 composite matches
        'unmatched_bank'   : DataFrame   – no match found
        'unmatched_ledger' : DataFrame
        'summary'          : dict
    }
    """
    if config is None:
        config = MatchConfig()

    bank   = standardize_bank(bank_df)
    ledger = standardize_ledger(ledger_df)

    # Pre-compute base-currency amounts once per row (uses description-embedded FX rates)
    bank   = _enrich_amounts(bank,   config)
    ledger = _enrich_amounts(ledger, config)

    # Auto-enable account weight when both sides have account data
    has_account = (
        bank['account'].notna().any()
        and (bank['account'] != 'NONE').any()
        and ledger['account'].notna().any()
        and (ledger['account'] != 'NONE').any()
    )
    if has_account and config.account_weight == 0.0:
        import dataclasses
        config = dataclasses.replace(config, account_weight=0.20)

    matched      = []
    near_matched = []
    composite    = []
    used_bank    = set()
    used_ledger  = set()

    # ── Pass 1: Exact / fast lookup ───────────────────────────
    # itertuples ~10x faster than iterrows — no pd.Series constructed per row.
    # Store only the index in ledger_map; fetch full row via .loc only on a hit.

    ledger_map: dict = {}
    for t in ledger.itertuples():
        key = (t.account, t.date, round(float(t.amount_base), 2))
        ledger_map.setdefault(key, []).append(t.Index)

    for t in bank.itertuples():
        if t.Index in used_bank:
            continue
        key = (t.account, t.date, round(float(t.amount_base), 2))
        if key in ledger_map and ledger_map[key]:
            l_idx = ledger_map[key].pop(0)
            b_row = bank.loc[t.Index]
            l_row = ledger.loc[l_idx]
            conf  = _confidence(b_row, l_row, config, has_account)
            matched.append(_build_match_row(b_row, l_row, conf, 'EXACT'))
            used_bank.add(t.Index)
            used_ledger.add(l_idx)

    # ── Pass 2: Near-match 1-to-1 ─────────────────────────────────────────
    # Dynamic performance thresholds
    skip_near      = len(bank) > 4000
    skip_composite = len(bank) > 2000

    if not skip_near:
        # Build amount-keyed index (index-only — no full Series stored in dict).
        # Key = amount_base rounded to nearest integer for O(1) bucket lookup.
        _ledger_amt_idx: dict = {}
        for t in ledger.itertuples():
            if t.Index in used_ledger:
                continue
            _ledger_amt_idx.setdefault(round(float(t.amount_base), 0), []).append(t.Index)

        for t in bank.itertuples():
            if t.Index in used_bank:
                continue
            b_base = float(t.amount_base)
            b_key  = round(b_base, 0)
            l_idxs = [i for i in _ledger_amt_idx.get(b_key, []) if i not in used_ledger]

            best_conf, best_l, best_l_idx = 0.0, None, None
            for l_idx in l_idxs:
                l_row = ledger.loc[l_idx]
                diff  = abs(b_base - float(l_row['amount_base']))
                if diff > config.amount_tolerance_abs:
                    if (config.amount_tolerance_pct == 0
                            or diff > abs(b_base) * config.amount_tolerance_pct):
                        continue
                b_row = bank.loc[t.Index]
                conf  = _confidence(b_row, l_row, config, has_account)
                if conf >= 0.50 and conf > best_conf:
                    best_conf, best_l, best_l_idx = conf, l_row, l_idx

            if best_l is not None:
                near_matched.append(_build_match_row(bank.loc[t.Index], best_l, best_conf, 'NEAR'))
                used_bank.add(t.Index)
                used_ledger.add(best_l_idx)

    # ── Pass 3: Composite 1-to-N ──────────────────────────────────────────
    # rem_l is computed ONCE before the loop, then refreshed only when a match
    # is found (rare).  Previously it was recomputed every iteration — O(n²).
    if config.composite_match and not skip_composite:
        rem_l = ledger[~ledger.index.isin(used_ledger)]
        for b_idx, b_row in bank.iterrows():
            if b_idx in used_bank:
                continue
            target = float(b_row['amount_base'])
            tol    = max(config.amount_tolerance_abs,
                         abs(target) * max(config.amount_tolerance_pct, 0.001))
            cands  = _composite_candidates_1_to_n(b_row, rem_l, config)
            result = _subset_sum_search(target, cands, min(config.composite_max_items, 2), tol)
            if result:
                composite.append(_build_composite_1_to_n(b_row, result))
                used_bank.add(b_idx)
                hit_idxs = {li for li, _ in result}
                used_ledger.update(hit_idxs)
                rem_l = rem_l[~rem_l.index.isin(hit_idxs)]  # update only on match

    # ── Pass 4: Composite N-to-1 ──────────────────────────────────────────
    if config.composite_match and not skip_composite:
        rem_b = bank[~bank.index.isin(used_bank)]
        for l_idx, l_row in ledger.iterrows():
            if l_idx in used_ledger:
                continue
            target = float(l_row['amount_base'])
            tol    = max(config.amount_tolerance_abs,
                         abs(target) * max(config.amount_tolerance_pct, 0.001))
            cands  = _composite_candidates_n_to_1(l_row, rem_b, config)
            result = _subset_sum_search(target, cands, min(config.composite_max_items, 2), tol)
            if result:
                composite.append(_build_composite_n_to_1(result, l_row))
                used_ledger.add(l_idx)
                hit_idxs = {bi for bi, _ in result}
                used_bank.update(hit_idxs)
                rem_b = rem_b[~rem_b.index.isin(hit_idxs)]  # update only on match

    # ── Collect unmatched ─────────────────────────────────────────────────
    unmatched_bank   = bank[~bank.index.isin(used_bank)].copy()
    unmatched_ledger = ledger[~ledger.index.isin(used_ledger)].copy()

    # ── Build DataFrames ──────────────────────────────────────────────────
    def _to_df(rows):
        return (pd.DataFrame(rows, columns=_MATCH_COLS)
                if rows else pd.DataFrame(columns=_MATCH_COLS))

    matched_df      = _to_df(matched)
    near_matched_df = _to_df(near_matched)
    composite_df    = _to_df(composite)

    export_cols = ['date', 'amount', 'reference', 'description']
    if has_account:
        export_cols.append('account')
    if bank['currency'].notna().any():
        export_cols.append('currency')

    summary = {
        'total_bank':        len(bank),
        'total_ledger':      len(ledger),
        'exact_matches':     len(matched_df),
        'near_matches':      len(near_matched_df),
        'composite_matches': len(composite_df),
        'unmatched_bank':    len(unmatched_bank),
        'unmatched_ledger':  len(unmatched_ledger),
        'match_rate': round(
            (len(matched_df) + len(near_matched_df) + len(composite_df))
            / max(len(bank), 1) * 100, 1
        ),
        'bank_total':    round(bank['amount'].sum(), 2),
        'ledger_total':  round(ledger['amount'].sum(), 2),
        'difference':    round(bank['amount'].sum() - ledger['amount'].sum(), 2),
        'has_account':   has_account,
        'has_currency':  (bank['currency'].notna().any()
                          or ledger['currency'].notna().any()),
        'base_currency': config.base_currency,
    }

    return {
        'matched':          matched_df,
        'near_matched':     near_matched_df,
        'composite':        composite_df,
        'unmatched_bank':   unmatched_bank[[c for c in export_cols if c in unmatched_bank.columns]],
        'unmatched_ledger': unmatched_ledger[[c for c in export_cols if c in unmatched_ledger.columns]],
        'summary':          summary,
    }
