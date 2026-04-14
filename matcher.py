"""
Bank Reconciliation Matching Engine - Phase 1
Matches bank transactions against ledger entries using multiple parameters.
"""

import pandas as pd
import numpy as np
from dataclasses import dataclass


def _parse_dates(series: pd.Series) -> pd.Series:
    """
    Parse dates robustly.
    Tries ISO8601 (YYYY-MM-DD) first; falls back to mixed/dayfirst
    for DD-MM-YYYY input if the ISO pass yields mostly NaT.
    """
    parsed = pd.to_datetime(series, format='ISO8601', errors='coerce')
    if parsed.isna().mean() > 0.5 and series.notna().any():
        parsed = pd.to_datetime(series, format='mixed', dayfirst=True, errors='coerce')
    return parsed


@dataclass
class MatchConfig:
    """Configuration for matching tolerances."""
    date_tolerance_days: int = 3          # days of slack allowed
    amount_tolerance_pct: float = 0.0     # 0 = exact amount match required
    reference_weight: float = 0.4
    amount_weight: float = 0.35
    date_weight: float = 0.25


def standardize_bank(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize bank statement columns to internal format."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    # Map common column name variants
    col_map = {
        'txn_date': 'date', 'transaction_date': 'date', 'value_date': 'date',
        'narration': 'description', 'particulars': 'description', 'remarks': 'description',
        'reference': 'reference', 'ref_no': 'reference', 'utr': 'reference',
        'amount': 'amount', 'debit': 'amount', 'withdrawal': 'amount',
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
    df['date'] = _parse_dates(df['date'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['reference'] = df['reference'].astype(str).str.strip().str.upper()
    df['description'] = (df['description'] if 'description' in df.columns else pd.Series([''] * len(df), index=df.index)).astype(str).str.strip().str.upper()
    df['source'] = 'BANK'
    df['original_index'] = df.index
    return df


def standardize_ledger(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize ledger/cashbook columns to internal format."""
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower()

    col_map = {
        'entry_date': 'date', 'posting_date': 'date', 'doc_date': 'date',
        'voucher_no': 'reference', 'document_no': 'reference', 'ref': 'reference',
        'party_name': 'description', 'vendor': 'description', 'payee': 'description',
        'amount': 'amount', 'net_amount': 'amount',
    }
    # Only rename cols whose target does not already exist, to avoid duplicate column names.
    safe_rename = {
        k: v for k, v in col_map.items()
        if k in df.columns and v not in df.columns
    }
    df = df.rename(columns=safe_rename)
    df['date'] = _parse_dates(df['date'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
    df['reference'] = df['reference'].astype(str).str.strip().str.upper()
    desc_col = df['description'] if 'description' in df.columns else pd.Series([''] * len(df), index=df.index)
    df['description'] = desc_col.astype(str).str.strip().str.upper()
    df['source'] = 'LEDGER'
    df['original_index'] = df.index
    return df


def _ref_similarity(ref1: str, ref2: str) -> float:
    """Check if references match. Returns 1.0 for exact, 0.5 for partial, 0.0 for no match."""
    if ref1 == ref2:
        return 1.0
    # Check if one contains the other (partial reference match)
    if ref1 in ref2 or ref2 in ref1:
        return 0.5
    return 0.0


def _date_score(date1, date2, tolerance_days: int) -> float:
    """Score date proximity. 1.0 = same day, 0.0 = beyond tolerance."""
    if pd.isna(date1) or pd.isna(date2):
        return 0.0
    diff = abs((date1 - date2).days)
    if diff == 0:
        return 1.0
    elif diff <= tolerance_days:
        return 1.0 - (diff / (tolerance_days + 1))
    return 0.0


def _amount_matches(amt1: float, amt2: float, tolerance_pct: float) -> bool:
    """Check if amounts match within tolerance."""
    if pd.isna(amt1) or pd.isna(amt2):
        return False
    if tolerance_pct == 0:
        return amt1 == amt2
    return abs(amt1 - amt2) <= abs(amt1) * tolerance_pct


def reconcile(bank_df: pd.DataFrame, ledger_df: pd.DataFrame, config: MatchConfig = None) -> dict:
    """
    Main reconciliation function.
    
    Returns dict with:
        - matched: DataFrame of exact matches (confidence >= 0.85)
        - near_matched: DataFrame of probable matches (0.50 <= confidence < 0.85)
        - unmatched_bank: DataFrame of bank items with no match
        - unmatched_ledger: DataFrame of ledger items with no match
        - summary: dict with counts and totals
    """
    if config is None:
        config = MatchConfig()

    bank = standardize_bank(bank_df)
    ledger = standardize_ledger(ledger_df)

    matched = []
    near_matched = []
    used_bank = set()
    used_ledger = set()

    # --- Pass 1: Exact reference + amount match ---
    for b_idx, b_row in bank.iterrows():
        if b_idx in used_bank:
            continue
        for l_idx, l_row in ledger.iterrows():
            if l_idx in used_ledger:
                continue
            if not _amount_matches(b_row['amount'], l_row['amount'], config.amount_tolerance_pct):
                continue

            ref_score = _ref_similarity(b_row['reference'], l_row['reference'])
            date_score = _date_score(b_row['date'], l_row['date'], config.date_tolerance_days)

            # Weighted confidence
            confidence = (
                config.reference_weight * ref_score +
                config.amount_weight * 1.0 +  # amount already matched
                config.date_weight * date_score
            )

            if confidence >= 0.85:
                matched.append(_build_match_row(b_row, l_row, confidence, 'EXACT'))
                used_bank.add(b_idx)
                used_ledger.add(l_idx)
                break

    # --- Pass 2: Near matches (amount match + date tolerance, weaker ref) ---
    for b_idx, b_row in bank.iterrows():
        if b_idx in used_bank:
            continue
        best_score = 0
        best_match = None
        best_l_idx = None

        for l_idx, l_row in ledger.iterrows():
            if l_idx in used_ledger:
                continue
            if not _amount_matches(b_row['amount'], l_row['amount'], config.amount_tolerance_pct):
                continue

            ref_score = _ref_similarity(b_row['reference'], l_row['reference'])
            date_score = _date_score(b_row['date'], l_row['date'], config.date_tolerance_days)

            confidence = (
                config.reference_weight * ref_score +
                config.amount_weight * 1.0 +
                config.date_weight * date_score
            )

            if confidence >= 0.50 and confidence > best_score:
                best_score = confidence
                best_match = l_row
                best_l_idx = l_idx

        if best_match is not None:
            near_matched.append(_build_match_row(b_row, best_match, best_score, 'NEAR'))
            used_bank.add(b_idx)
            used_ledger.add(best_l_idx)

    # --- Collect unmatched ---
    unmatched_bank = bank[~bank.index.isin(used_bank)].copy()
    unmatched_ledger = ledger[~ledger.index.isin(used_ledger)].copy()

    # --- Build result DataFrames ---
    match_cols = [
        'bank_date', 'bank_amount', 'bank_reference', 'bank_description',
        'ledger_date', 'ledger_amount', 'ledger_reference', 'ledger_description',
        'confidence', 'match_type'
    ]

    matched_df = pd.DataFrame(matched, columns=match_cols) if matched else pd.DataFrame(columns=match_cols)
    near_matched_df = pd.DataFrame(near_matched, columns=match_cols) if near_matched else pd.DataFrame(columns=match_cols)

    summary = {
        'total_bank': len(bank),
        'total_ledger': len(ledger),
        'exact_matches': len(matched_df),
        'near_matches': len(near_matched_df),
        'unmatched_bank': len(unmatched_bank),
        'unmatched_ledger': len(unmatched_ledger),
        'match_rate': round((len(matched_df) + len(near_matched_df)) / max(len(bank), 1) * 100, 1),
        'bank_total': bank['amount'].sum(),
        'ledger_total': ledger['amount'].sum(),
        'difference': bank['amount'].sum() - ledger['amount'].sum(),
    }

    return {
        'matched': matched_df,
        'near_matched': near_matched_df,
        'unmatched_bank': unmatched_bank[['date', 'amount', 'reference', 'description']],
        'unmatched_ledger': unmatched_ledger[['date', 'amount', 'reference', 'description']],
        'summary': summary,
    }


def _build_match_row(bank_row, ledger_row, confidence, match_type):
    """Build a single match result row."""
    return [
        bank_row['date'], bank_row['amount'], bank_row['reference'], bank_row['description'],
        ledger_row['date'], ledger_row['amount'], ledger_row['reference'], ledger_row['description'],
        round(confidence, 2), match_type,
    ]
