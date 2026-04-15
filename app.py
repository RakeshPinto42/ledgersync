"""
LedgerSync — Bank Reconciliation Tool
Run: streamlit run app.py
"""

import io
import time
import html as _html
import streamlit as st
import pandas as pd
from matcher import reconcile, MatchConfig
from month_end import generate_month_end_recon

# ── Safety cap: rows processed per file ──
MAX_ROWS = 5000

# ── Page Config ──
st.set_page_config(
    page_title="LedgerSync",
    page_icon="⬡",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ══════════════════════════════════════════════
#  GLOBAL CSS  — Web3 / Fintech Premium Theme
# ══════════════════════════════════════════════
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

.stApp {
    background: #0B1220;
    background-image:
        radial-gradient(ellipse 90% 55% at 50% -8%, rgba(59,130,246,0.18) 0%, transparent 58%),
        radial-gradient(ellipse 45% 35% at 88% 12%, rgba(99,102,241,0.09) 0%, transparent 50%);
}
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 0.5rem !important; padding-bottom: 3rem !important; }

/* ── Settings expander panel ── */
.streamlit-expanderHeader {
    background: rgba(17,24,39,0.88) !important;
    border: 1px solid rgba(59,130,246,0.24) !important;
    border-radius: 14px !important;
    color: #94a3b8 !important;
    font-size: 0.84rem !important;
    font-weight: 500 !important;
    padding: 0.75rem 1.2rem !important;
    transition: border-color 0.2s, color 0.2s !important;
}
.streamlit-expanderHeader:hover {
    border-color: rgba(59,130,246,0.52) !important;
    color: #e2e8f0 !important;
}
.streamlit-expanderContent {
    background: rgba(11,18,32,0.95) !important;
    border: 1px solid rgba(59,130,246,0.18) !important;
    border-top: none !important;
    border-radius: 0 0 14px 14px !important;
    padding: 1.4rem 1.8rem 1.6rem !important;
}

/* ── Caption / section labels inside expander ── */
[data-testid="stCaptionContainer"] p, small {
    color: #475569 !important;
    font-size: 0.62rem !important;
    letter-spacing: 0.09em !important;
    text-transform: uppercase !important;
    font-weight: 600 !important;
}

/* ── Slider ── */
[data-testid="stSlider"] > div > div > div {
    background: linear-gradient(90deg, #3B82F6, #1D4ED8) !important;
}

h1,h2,h3,h4,h5,h6,p,label,.stMarkdown { color: #e2e8f0; }
hr { border-color: rgba(59,130,246,0.15) !important; }

/* ── Inputs ── */
[data-testid="stNumberInput"] input,
[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea {
    background: rgba(17,24,39,0.8) !important;
    border: 1px solid rgba(59,130,246,0.28) !important;
    border-radius: 9px !important;
    color: #e2e8f0 !important;
    transition: border-color 0.2s !important;
}
[data-testid="stNumberInput"] input:focus,
[data-testid="stTextInput"] input:focus,
[data-testid="stTextArea"] textarea:focus {
    border-color: rgba(59,130,246,0.65) !important;
}
[data-testid="stSelectbox"] > div > div {
    background: rgba(17,24,39,0.8) !important;
    border: 1px solid rgba(59,130,246,0.25) !important;
    border-radius: 9px !important;
    color: #e2e8f0 !important;
}
/* radio */
[data-testid="stRadio"] label { color: #94a3b8 !important; font-size: 0.82rem !important; }
[data-testid="stRadio"] [data-testid="stMarkdownContainer"] p { color: #94a3b8 !important; }

/* ── File uploader ── */
[data-testid="stFileUploader"] {
    background: rgba(17,24,39,0.72) !important;
    border: 1.5px dashed rgba(59,130,246,0.32) !important;
    border-radius: 14px !important;
    padding: 1.1rem !important;
    backdrop-filter: blur(10px);
    transition: border-color 0.2s, background 0.2s !important;
}
[data-testid="stFileUploader"]:hover {
    border-color: rgba(59,130,246,0.68) !important;
    background: rgba(30,41,59,0.55) !important;
}
[data-testid="stFileUploader"] * { color: #cbd5e1 !important; }

/* ── Primary CTA button ── */
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, #3B82F6 0%, #1D4ED8 100%) !important;
    color: #ffffff !important;
    border: none !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
    font-size: 1rem !important;
    padding: 0.75rem 2rem !important;
    box-shadow: 0 4px 24px rgba(59,130,246,0.42), inset 0 1px 0 rgba(255,255,255,0.08) !important;
    letter-spacing: 0.01em !important;
    transition: box-shadow 0.2s ease, transform 0.15s ease !important;
}
.stButton > button[kind="primary"]:hover {
    box-shadow: 0 6px 32px rgba(59,130,246,0.62), inset 0 1px 0 rgba(255,255,255,0.08) !important;
    transform: translateY(-2px) !important;
}
.stButton > button[kind="primary"]:active { transform: translateY(0) !important; }
/* secondary / download buttons */
.stButton > button, .stDownloadButton > button {
    background: rgba(17,24,39,0.75) !important;
    color: #e2e8f0 !important;
    border: 1px solid rgba(59,130,246,0.28) !important;
    border-radius: 10px !important;
    font-weight: 500 !important;
    transition: background 0.2s, border-color 0.2s !important;
}
.stButton > button:hover, .stDownloadButton > button:hover {
    background: rgba(59,130,246,0.12) !important;
    border-color: rgba(59,130,246,0.55) !important;
}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"] {
    background: transparent !important;
    border-bottom: 1px solid rgba(59,130,246,0.18) !important;
    gap: 0.15rem;
}
.stTabs [data-baseweb="tab"] {
    background: transparent !important;
    color: #475569 !important;
    border: none !important;
    border-radius: 8px 8px 0 0 !important;
    font-size: 0.8rem !important;
    font-weight: 500 !important;
    padding: 0.5rem 0.9rem !important;
    transition: color 0.15s, background 0.15s !important;
}
.stTabs [data-baseweb="tab"]:hover {
    color: #94a3b8 !important;
    background: rgba(59,130,246,0.06) !important;
}
.stTabs [aria-selected="true"] {
    color: #a5b4fc !important;
    background: rgba(59,130,246,0.1) !important;
    border-bottom: 2px solid #3B82F6 !important;
}

/* ── DataFrames ── */
.stDataFrame, [data-testid="stDataFrame"] {
    background: rgba(11,18,32,0.9) !important;
    border: 1px solid rgba(59,130,246,0.18) !important;
    border-radius: 12px !important;
    overflow: hidden;
}

/* ── Inline expanders (preview) ── */
details > summary {
    background: rgba(17,24,39,0.7) !important;
    border: 1px solid rgba(59,130,246,0.2) !important;
    border-radius: 10px !important;
    color: #94a3b8 !important;
}

/* ── Alerts ── */
.stAlert {
    background: rgba(17,24,39,0.8) !important;
    border-radius: 10px !important;
    border: 1px solid rgba(59,130,246,0.22) !important;
}
.stSpinner > div { border-top-color: #3B82F6 !important; }

/* ── KPI cards ── */
.kpi-card {
    background: rgba(17,24,39,0.75);
    border: 1px solid rgba(59,130,246,0.22);
    border-radius: 18px;
    padding: 1.5rem 1.6rem 1.3rem;
    backdrop-filter: blur(16px);
    box-shadow: 0 4px 32px rgba(0,0,0,0.4), inset 0 1px 0 rgba(255,255,255,0.04);
    position: relative; overflow: hidden;
    transition: border-color 0.25s, box-shadow 0.25s, transform 0.2s;
}
.kpi-card::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: linear-gradient(90deg, #3B82F6 0%, #6366F1 50%, transparent 100%);
    border-radius: 18px 18px 0 0;
}
.kpi-card:hover {
    border-color: rgba(59,130,246,0.48);
    box-shadow: 0 8px 40px rgba(59,130,246,0.18), inset 0 1px 0 rgba(255,255,255,0.06);
    transform: translateY(-1px);
}
.kpi-label {
    font-size: 0.66rem; font-weight: 700; letter-spacing: 0.1em;
    text-transform: uppercase; color: #475569; margin-bottom: 0.7rem;
}
.kpi-value {
    font-size: 2.1rem; font-weight: 800; line-height: 1;
    background: linear-gradient(135deg, #f1f5f9 0%, #a5b4fc 100%);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent; background-clip: text;
    margin-bottom: 0.3rem;
}
.kpi-sub { font-size: 0.72rem; color: #475569; }

/* ── Section pill header ── */
.section-pill {
    display: inline-flex; align-items: center; gap: 0.4rem;
    background: rgba(59,130,246,0.1); border: 1px solid rgba(59,130,246,0.28);
    border-radius: 20px; padding: 0.22rem 0.9rem;
    font-size: 0.67rem; font-weight: 700;
    letter-spacing: 0.09em; text-transform: uppercase; color: #a5b4fc; margin-bottom: 0.6rem;
}

/* ── Badges ── */
.badge-matched   { display:inline-block;background:rgba(16,185,129,0.15);border:1px solid rgba(16,185,129,0.35);color:#34d399;border-radius:6px;padding:0.1rem 0.55rem;font-size:0.7rem;font-weight:600; }
.badge-review    { display:inline-block;background:rgba(245,158,11,0.15);border:1px solid rgba(245,158,11,0.35);color:#fbbf24;border-radius:6px;padding:0.1rem 0.55rem;font-size:0.7rem;font-weight:600; }
.badge-composite { display:inline-block;background:rgba(139,92,246,0.15);border:1px solid rgba(139,92,246,0.35);color:#c084fc;border-radius:6px;padding:0.1rem 0.55rem;font-size:0.7rem;font-weight:600; }
.badge-exception { display:inline-block;background:rgba(239,68,68,0.15);border:1px solid rgba(239,68,68,0.35);color:#f87171;border-radius:6px;padding:0.1rem 0.55rem;font-size:0.7rem;font-weight:600; }

/* ── Upload source cards ── */
.upload-label {
    font-size: 0.68rem; font-weight: 700; letter-spacing: 0.1em;
    text-transform: uppercase; color: #475569; margin-bottom: 0.6rem;
}

/* ── Mapper card ── */
.mapper-title { font-size:0.67rem;font-weight:700;letter-spacing:0.1em;text-transform:uppercase;color:#3B82F6;margin-bottom:0.85rem; }

/* ── BRS Statement ── */
.brs-wrap {
    background: rgba(17,24,39,0.72);
    border: 1px solid rgba(59,130,246,0.2);
    border-radius: 18px;
    padding: 1.8rem 2.2rem;
    backdrop-filter: blur(16px);
    width: 100%;
    box-shadow: 0 4px 32px rgba(0,0,0,0.35);
}
.brs-title {
    font-size: 1rem;
    font-weight: 700;
    color: #e2e8f0;
    letter-spacing: -0.01em;
    margin-bottom: 0.2rem;
}
.brs-subtitle {
    font-size: 0.73rem;
    color: #475569;
    margin-bottom: 0;
    letter-spacing: 0.02em;
}
.brs-col-header {
    display: flex;
    justify-content: space-between;
    padding: 0.5rem 0 0.5rem;
    border-bottom: 1px solid rgba(59,130,246,0.3);
    margin: 1rem 0 0.1rem;
}
.brs-col-header span {
    font-size: 0.63rem;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: #475569;
}
.brs-col-header span:last-child {
    min-width: 140px;
    text-align: right;
}
.brs-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 0.5rem 0;
    border-bottom: 1px solid rgba(59,130,246,0.07);
    font-size: 0.86rem;
}
.brs-row-label { color: #94a3b8; flex: 1; padding-right: 1rem; }
.brs-tag {
    font-size: 0.6rem;
    font-weight: 700;
    letter-spacing: 0.07em;
    margin-right: 0.45rem;
    padding: 0.15rem 0.4rem;
    border-radius: 4px;
    vertical-align: middle;
}
.brs-tag-add  { background: rgba(16,185,129,0.15); color: #34d399; }
.brs-tag-less { background: rgba(239,68,68,0.15);  color: #f87171; }
.brs-row-amt {
    font-weight: 600;
    color: #cbd5e1;
    min-width: 140px;
    text-align: right;
    font-variant-numeric: tabular-nums;
    font-size: 0.9rem;
}
.brs-divider  { border: none; border-top: 1px solid rgba(59,130,246,0.22); margin: 0.5rem 0; }
.brs-total-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    padding: 0.65rem 0.75rem;
    background: rgba(59,130,246,0.09);
    border: 1px solid rgba(59,130,246,0.18);
    border-radius: 8px;
    margin: 0.35rem 0;
    font-size: 0.9rem;
    font-weight: 600;
}
.brs-total-label { color: #c7d2fe; flex: 1; }
.brs-total-amt   { color: #a5b4fc; font-variant-numeric: tabular-nums; min-width: 140px; text-align: right; }
.brs-diff-zero    { color: #34d399 !important; }
.brs-diff-nonzero { color: #f87171 !important; }
.brs-note { font-size: 0.7rem; color: #475569; font-weight: 400; margin-left: 0.5rem; }

/* ── Footer ── */
.xbp-footer {
    text-align: center; color: #1e293b; font-size: 0.7rem;
    letter-spacing: 0.07em; padding: 2.5rem 0 1rem;
    border-top: 1px solid rgba(59,130,246,0.08); margin-top: 3.5rem;
}
.xbp-footer span {
    background: linear-gradient(90deg, #3B82F6, #6366F1);
    -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    background-clip: text; font-weight: 700;
}
</style>
""", unsafe_allow_html=True)


# ══════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════

# ── Column keyword auto-detection ──
_COL_KW = {
    'date':        ['date','dt','txn_date','value_date','transaction_date',
                    'posting_date','entry_date','doc_date','trade_date','tran_date'],
    'amount':      ['amount','amt','net_amount','net','value'],
    'debit':       ['debit','dr','withdrawal','outflow','paid','debit_amount'],
    'credit':      ['credit','cr','deposit','inflow','received','credit_amount'],
    'reference':   ['reference','ref','ref_no','utr','voucher_no','voucher',
                    'document_no','doc_no','cheque','chq','txn_id','trans_id'],
    'description': ['description','narration','particulars','remarks','details',
                    'party_name','vendor','payee','memo','narrative'],
    'currency':    ['currency','ccy','curr','fx','foreign_currency'],
    'account':     ['account','account_no','acc_no','account_number',
                    'bank_account','gl_account','acct','ledger_account'],
}

def _auto_detect(columns: list, target: str):
    cols_norm = {c.strip().lower().replace(' ','_').replace('-','_'): c for c in columns}
    for kw in _COL_KW.get(target, []):
        for norm, orig in cols_norm.items():
            if kw == norm or kw in norm or norm in kw:
                return orig
    return None


def _col_mapper_ui(df: pd.DataFrame, label: str, key_prefix: str) -> dict:
    """
    Extended column mapper:
      • Row 1: Date*, Reference, Description
      • Row 2: Amount format radio → single signed | separate Debit + Credit
      • Row 3: Currency (optional), Account Number (optional)
    """
    cols = list(df.columns)
    NONE = "— none —"

    st.markdown(f'<div class="mapper-title">{label}</div>', unsafe_allow_html=True)

    # Row 1 ─ date, reference, description
    r1a, r1b, r1c = st.columns(3, gap="small")

    def _pick(container, field, required=True, extra_label=""):
        guess   = _auto_detect(cols, field)
        options = cols if required else [NONE] + cols
        try:
            default = options.index(guess) if guess else 0
        except ValueError:
            default = 0
        lbl = f"{'* ' if required else ''}{field.capitalize()}{extra_label}"
        return container.selectbox(lbl, options=options, index=default,
                                   key=f"{key_prefix}_{field}")

    date_col = _pick(r1a, 'date')
    ref_col  = _pick(r1b, 'reference', required=False)
    desc_col = _pick(r1c, 'description', required=False)

    # Row 2 ─ amount format
    amt_type = st.radio(
        "Amount format",
        ["Single signed column", "Separate Debit + Credit columns"],
        horizontal=True,
        key=f"{key_prefix}_amt_type",
    )
    if amt_type == "Single signed column":
        amt_col    = _pick(st, 'amount', required=True, extra_label=" *")
        debit_col  = None
        credit_col = None
    else:
        r2a, r2b = st.columns(2, gap="small")
        debit_col  = _pick(r2a, 'debit',  required=True,
                           extra_label=" * (outflows, positive numbers)")
        credit_col = _pick(r2b, 'credit', required=True,
                           extra_label=" * (inflows, positive numbers)")
        amt_col = None

    # Row 3 ─ currency, account
    r3a, r3b = st.columns(2, gap="small")
    ccy_col  = _pick(r3a, 'currency', required=False)
    acct_col = _pick(r3b, 'account',  required=False)

    return {
        'date':        date_col,
        'amount':      amt_col,
        'debit':       debit_col,
        'credit':      credit_col,
        'reference':   ref_col  if ref_col  != NONE else None,
        'description': desc_col if desc_col != NONE else None,
        'currency':    ccy_col  if ccy_col  != NONE else None,
        'account':     acct_col if acct_col != NONE else None,
    }


def _preprocess_df(df_raw: pd.DataFrame, mapping: dict) -> pd.DataFrame | None:
    """
    Apply user column mapping and handle debit/credit combining.
    Returns a DataFrame with standard column names ready for reconcile(),
    or None if required columns are missing (errors shown via st.error).
    """
    df = df_raw.copy()

    # Normalize column names (defensive — load_file already does this)
    df.columns = df.columns.str.strip().str.lower()

    # ── Validate required date column ──
    date_col = mapping.get('date')
    if not date_col:
        st.error("Date column not mapped. Please select a date column.")
        return None
    if date_col not in df.columns:
        st.error(
            f"Date column **'{date_col}'** not found in the file. "
            "Please verify the column mapping."
        )
        return None

    # ── Rename standard optional fields ──
    for field in ('date', 'reference', 'description', 'currency', 'account'):
        col = mapping.get(field)
        if col and col in df.columns and col != field:
            df = df.rename(columns={col: field})

    # ── Strip whitespace from all string columns ──
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].astype(str).str.strip().replace({'nan': pd.NA, 'None': pd.NA, '': pd.NA})

    # ── Amount handling ──
    if mapping.get('amount'):
        src = mapping['amount']
        if src not in df.columns and src != 'amount':
            st.error(
                f"Amount column **'{src}'** not found. Please check mapping."
            )
            return None
        if src in df.columns and src != 'amount':
            df = df.rename(columns={src: 'amount'})
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')

    elif mapping.get('debit') and mapping.get('credit'):
        dr_col, cr_col = mapping['debit'], mapping['credit']
        for col_name, label in ((dr_col, 'Debit'), (cr_col, 'Credit')):
            if col_name not in df.columns:
                st.error(
                    f"{label} column **'{col_name}'** not found. Please check mapping."
                )
                return None
        dr = pd.to_numeric(df[dr_col], errors='coerce').fillna(0)
        cr = pd.to_numeric(df[cr_col], errors='coerce').fillna(0)
        df['amount'] = cr - dr   # net: inflows positive, outflows negative

    elif mapping.get('debit'):
        dr_col = mapping['debit']
        if dr_col not in df.columns:
            st.error(f"Debit column **'{dr_col}'** not found. Please check mapping.")
            return None
        df['amount'] = -pd.to_numeric(df[dr_col], errors='coerce')

    elif mapping.get('credit'):
        cr_col = mapping['credit']
        if cr_col not in df.columns:
            st.error(f"Credit column **'{cr_col}'** not found. Please check mapping.")
            return None
        df['amount'] = pd.to_numeric(df[cr_col], errors='coerce')

    # ── Final guard: amount column must exist ──
    if 'amount' not in df.columns:
        st.error("Amount column not found after mapping. Please check your column selections.")
        return None

    # ── Warn about rows with invalid amounts ──
    bad_amt = df['amount'].isna().sum()
    if bad_amt == len(df):
        st.error(
            "No data found after processing. All amount values are invalid or non-numeric. "
            "Please verify the file format and column mappings."
        )
        return None
    if bad_amt > 0:
        st.warning(
            f"{bad_amt:,} row(s) have non-numeric amounts and will be treated as zero / skipped."
        )

    return df


def _parse_fx_rates(text: str) -> dict:
    """Parse 'USD=84.5, EUR=91.2' or newline-separated into {CCY: rate}."""
    rates = {}
    for part in text.replace('\n', ',').split(','):
        part = part.strip()
        if '=' in part:
            ccy, rate = part.split('=', 1)
            try:
                rates[ccy.strip().upper()] = float(rate.strip())
            except ValueError:
                pass
    return rates


@st.cache_data(show_spinner=False)
def _parse_file_bytes(file_bytes: bytes, filename: str) -> pd.DataFrame | None:
    """Cache-friendly parser: takes raw bytes so Streamlit can hash the input."""
    try:
        name = filename.lower()
        if name.endswith('.csv'):
            df = pd.read_csv(io.BytesIO(file_bytes))
        elif name.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(io.BytesIO(file_bytes))
        else:
            return None
        # Normalize column names immediately after load
        df.columns = df.columns.str.strip().str.lower()
        return df
    except Exception:
        return None


def load_file(uploaded_file) -> pd.DataFrame | None:
    """
    Load an uploaded file, normalize columns, validate, and enforce the row cap.
    Returns None (and shows a user-friendly error) on any failure.
    """
    try:
        df = _parse_file_bytes(uploaded_file.getvalue(), uploaded_file.name)
    except Exception:
        st.error("Error reading file. Please verify the file is not corrupted and try again.")
        return None

    if df is None:
        st.error(
            f"Invalid file format for **{uploaded_file.name}**. "
            "Please upload a CSV or Excel file (.csv, .xlsx, .xls)."
        )
        return None

    if df.empty:
        st.error(f"No data found in **{uploaded_file.name}**. The file appears to be empty.")
        return None

    if len(df) > MAX_ROWS:
        st.warning(
            f"**{uploaded_file.name}** has {len(df):,} rows. "
            f"Processing is limited to the first **{MAX_ROWS:,} rows** for performance. "
            "Split the file into smaller batches for full processing."
        )
        df = df.head(MAX_ROWS)

    return df


def safe_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index(drop=True)
    for col in df.columns:
        if (pd.api.types.is_string_dtype(df[col])
                and pd.api.types.is_extension_array_dtype(df[col])):
            df[col] = df[col].astype(object)
    return df


def _render_brs(summary: dict, results: dict, period_label: str, opening_balance: float):
    bank_bal   = summary['bank_total']
    ledger_bal = summary['ledger_total']

    unmatched_bank_amt   = (results['unmatched_bank']['amount'].sum()
                            if not results['unmatched_bank'].empty else 0.0)
    unmatched_ledger_amt = (results['unmatched_ledger']['amount'].sum()
                            if not results['unmatched_ledger'].empty else 0.0)

    adjusted_bank = bank_bal + unmatched_ledger_amt - unmatched_bank_amt
    recon_diff    = adjusted_bank - ledger_bal

    ccy = summary.get('base_currency', 'INR')

    def _fmt(v):
        try:
            sym = '₹' if ccy == 'INR' else f'{ccy} '
            return f"{sym}{float(v):,.2f}"
        except (TypeError, ValueError):
            return '—'

    diff_class = "brs-diff-zero" if abs(recon_diff) < 0.01 else "brs-diff-nonzero"
    diff_note  = "Fully reconciled" if abs(recon_diff) < 0.01 else "Residual — review near/composite matches"

    nb_count = summary['unmatched_bank']
    nl_count = summary['unmatched_ledger']

    # HTML-escape all user-provided strings to prevent broken markup
    safe_period   = _html.escape(str(period_label))
    safe_ccy      = _html.escape(str(ccy))
    safe_prepared = _html.escape(pd.Timestamp.today().strftime('%d %b %Y'))
    safe_note     = _html.escape(diff_note)

    # Build HTML as a variable — then render with st.markdown once.
    # Splitting into a string first avoids f-string interaction with any
    # special characters the user may have typed in the period field.
    brs_html = (
        '<div class="brs-wrap">'
        '<div class="brs-title">Bank Reconciliation Statement</div>'
        f'<div class="brs-subtitle">'
        f'Period: {safe_period} &nbsp;·&nbsp; '
        f'Prepared: {safe_prepared} &nbsp;·&nbsp; '
        f'Base currency: {safe_ccy}'
        f'</div>'

        '<div class="brs-col-header">'
        '<span>Particulars</span>'
        f'<span>Amount ({safe_ccy})</span>'
        '</div>'

        '<div class="brs-row">'
        '<span class="brs-row-label">Opening Balance as per Books (Ledger)</span>'
        f'<span class="brs-row-amt">{_fmt(opening_balance)}</span>'
        '</div>'

        '<div class="brs-row">'
        '<span class="brs-row-label">Balance as per Bank Statement (closing)</span>'
        f'<span class="brs-row-amt">{_fmt(bank_bal)}</span>'
        '</div>'

        '<hr class="brs-divider">'

        '<div class="brs-row">'
        '<span class="brs-row-label">'
        '<span class="brs-tag brs-tag-add">ADD</span>'
        f'Outstanding Ledger / ERP Entries '
        f'<span class="brs-note">({nl_count} items — booked, not yet cleared by bank)</span>'
        '</span>'
        f'<span class="brs-row-amt">+ {_fmt(unmatched_ledger_amt)}</span>'
        '</div>'

        '<div class="brs-row">'
        '<span class="brs-row-label">'
        '<span class="brs-tag brs-tag-less">LESS</span>'
        f'Unrecorded Bank Entries '
        f'<span class="brs-note">({nb_count} items — in bank statement, not yet posted)</span>'
        '</span>'
        f'<span class="brs-row-amt">&#8722; {_fmt(unmatched_bank_amt)}</span>'
        '</div>'

        '<hr class="brs-divider">'

        '<div class="brs-total-row">'
        '<span class="brs-total-label">Adjusted Bank Balance</span>'
        f'<span class="brs-total-amt">{_fmt(adjusted_bank)}</span>'
        '</div>'

        '<div class="brs-row" style="margin-top:0.5rem;">'
        '<span class="brs-row-label">Balance as per Books / Ledger</span>'
        f'<span class="brs-row-amt">{_fmt(ledger_bal)}</span>'
        '</div>'

        '<hr class="brs-divider">'

        f'<div class="brs-total-row">'
        f'<span class="brs-total-label {diff_class}">'
        f'Net Difference &nbsp;'
        f'<span style="font-size:0.7rem;font-weight:400;color:#475569;">{safe_note}</span>'
        f'</span>'
        f'<span class="brs-total-amt {diff_class}">{_fmt(recon_diff)}</span>'
        '</div>'

        '</div>'
    )

    st.markdown(brs_html, unsafe_allow_html=True)


def _per_account_summary(results: dict) -> pd.DataFrame | None:
    """Build a per-account reconciliation summary from combined results.
    Returns None when no account data is present in results."""
    rows: dict = {}

    def _row(acct):
        k = str(acct) if (acct is not None and str(acct).strip()) else '(unspecified)'
        if k not in rows:
            rows[k] = {
                'Account': k,
                'Matched': 0, 'Possible Matches': 0, 'Split Matches': 0,
                'Unmatched Bank': 0, 'Unmatched Ledger': 0,
                'Bank Total': 0.0, 'Ledger Total': 0.0,
            }
        return rows[k]

    df = results['matched']
    if not df.empty and 'bank_account' in df.columns:
        for acct, grp in df.groupby('bank_account', dropna=False):
            r = _row(acct)
            r['Matched'] += len(grp)
            r['Bank Total'] += pd.to_numeric(grp['bank_amount'], errors='coerce').sum()

    df = results['near_matched']
    if not df.empty and 'bank_account' in df.columns:
        for acct, grp in df.groupby('bank_account', dropna=False):
            r = _row(acct)
            r['Possible Matches'] += len(grp)
            r['Bank Total'] += pd.to_numeric(grp['bank_amount'], errors='coerce').sum()

    df = results.get('composite', pd.DataFrame())
    if not df.empty and 'bank_account' in df.columns:
        for acct, grp in df.groupby('bank_account', dropna=False):
            r = _row(acct)
            r['Split Matches'] += len(grp)
            r['Bank Total'] += pd.to_numeric(grp['bank_amount'], errors='coerce').sum()

    df = results['unmatched_bank']
    if not df.empty and 'account' in df.columns:
        for acct, grp in df.groupby('account', dropna=False):
            r = _row(acct)
            r['Unmatched Bank'] += len(grp)
            r['Bank Total'] += pd.to_numeric(grp['amount'], errors='coerce').sum()

    df = results['unmatched_ledger']
    if not df.empty and 'account' in df.columns:
        for acct, grp in df.groupby('account', dropna=False):
            r = _row(acct)
            r['Unmatched Ledger'] += len(grp)
            r['Ledger Total'] += pd.to_numeric(grp['amount'], errors='coerce').sum()

    if not rows:
        return None

    out = pd.DataFrame(list(rows.values()))
    out['Bank Total']   = out['Bank Total'].round(2)
    out['Ledger Total'] = out['Ledger Total'].round(2)
    out['Difference']   = (out['Bank Total'] - out['Ledger Total']).round(2)
    return out


def _combine_account_results(per_account: dict) -> dict:
    """Merge per-account reconcile() outputs into one results dict
    that is structurally identical to a single reconcile() call."""
    if not per_account:
        return {}

    def _cat(key):
        frames = [r[key] for r in per_account.values()
                  if not r.get(key, pd.DataFrame()).empty]
        return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

    comp_frames = [r.get('composite', pd.DataFrame()) for r in per_account.values()
                   if not r.get('composite', pd.DataFrame()).empty]

    combined = {
        'matched':          _cat('matched'),
        'near_matched':     _cat('near_matched'),
        'unmatched_bank':   _cat('unmatched_bank'),
        'unmatched_ledger': _cat('unmatched_ledger'),
        'composite':        pd.concat(comp_frames, ignore_index=True) if comp_frames else pd.DataFrame(),
    }

    # Aggregate summary fields
    em = sum(r['summary']['exact_matches']            for r in per_account.values())
    nm = sum(r['summary']['near_matches']             for r in per_account.values())
    cm = sum(r['summary'].get('composite_matches', 0) for r in per_account.values())
    ub = sum(r['summary']['unmatched_bank']           for r in per_account.values())
    ul = sum(r['summary']['unmatched_ledger']         for r in per_account.values())
    bt = sum(r['summary']['bank_total']               for r in per_account.values())
    lt = sum(r['summary']['ledger_total']             for r in per_account.values())

    total_bank   = em + nm + cm + ub
    matched_bank = em + nm + cm
    first        = next(iter(per_account.values()))['summary']

    combined['summary'] = {
        'bank_total':        round(bt, 2),
        'ledger_total':      round(lt, 2),
        'difference':        round(bt - lt, 2),
        'total_bank':        total_bank,
        'total_ledger':      em + nm + cm + ul,
        'exact_matches':     em,
        'near_matches':      nm,
        'composite_matches': cm,
        'unmatched_bank':    ub,
        'unmatched_ledger':  ul,
        'match_rate':        round(matched_bank / total_bank * 100, 1) if total_bank else 0.0,
        'has_account':       True,
        'has_currency':      first.get('has_currency', False),
        'base_currency':     first.get('base_currency', 'INR'),
    }

    return combined


# ══════════════════════════════════════════════
#  HEADER
# ══════════════════════════════════════════════
st.markdown(
    '<div style="padding:2.8rem 0 2rem;text-align:center;">'
    '<div style="display:inline-flex;align-items:center;gap:0.55rem;margin-bottom:0.55rem;">'
    '<span style="font-size:1.6rem;filter:drop-shadow(0 0 8px rgba(99,102,241,0.5));">⬡</span>'
    '<span style="font-size:2.9rem;font-weight:800;letter-spacing:-0.04em;line-height:1;'
    'background:linear-gradient(135deg,#f1f5f9 30%,#a5b4fc 100%);'
    '-webkit-background-clip:text;-webkit-text-fill-color:transparent;'
    'background-clip:text;">LedgerSync</span>'
    '</div>'
    '<div style="font-size:0.78rem;font-weight:600;letter-spacing:0.14em;'
    'text-transform:uppercase;color:#475569;margin-bottom:0.3rem;">Financial Intelligence Platform</div>'
    '<div style="display:inline-flex;align-items:center;gap:0.4rem;'
    'background:rgba(59,130,246,0.08);border:1px solid rgba(59,130,246,0.18);'
    'border-radius:20px;padding:0.18rem 0.75rem;margin-top:0.2rem;">'
    '<span style="width:6px;height:6px;background:#34d399;border-radius:50%;'
    'display:inline-block;box-shadow:0 0 6px #34d399;"></span>'
    '<span style="font-size:0.65rem;font-weight:600;letter-spacing:0.06em;'
    'text-transform:uppercase;color:#64748b;">Live · by R@k</span>'
    '</div>'
    '</div>',
    unsafe_allow_html=True,
)


# ══════════════════════════════════════════════
#  SETTINGS  (replaces sidebar — expander panel)
# ══════════════════════════════════════════════

# Smart defaults — always defined so the rest of the app has valid values
# even if the expander is collapsed without changes.
advanced_mode   = False
opening_balance = 0.0
period_label    = "Current Period"
date_tolerance  = 3
amount_tol_abs  = 0.01
ref_weight      = 0.40
amt_weight      = 0.35
date_weight     = 0.25
composite_match = True
composite_max   = 5
composite_slack = 14
base_currency   = 'INR'
fx_rates        = _parse_fx_rates(
    "USD=84.50\nEUR=91.20\nGBP=107.80\nAED=23.00\nSGD=63.00\nAUD=55.00\nJPY=0.56"
)

with st.expander("⚙️  Settings & Configuration", expanded=False):

    # ── Row 1: Core BRS settings + mode toggle ──
    cfg1, cfg2, cfg3 = st.columns([1.3, 1.3, 0.8])
    with cfg1:
        opening_balance = st.number_input(
            "💰  Opening Ledger Balance",
            value=0.0, step=1000.0, format="%.2f",
            help="Ledger balance at the start of the reconciliation period",
        )
    with cfg2:
        period_label = st.text_input(
            "📅  Period Label",
            value="Current Period",
            help="e.g. April 2026 — used in BRS header and export filename",
        )
    with cfg3:
        st.markdown('<div style="margin-top:1.9rem;"></div>', unsafe_allow_html=True)
        advanced_mode = st.toggle(
            "Advanced Mode",
            value=False,
            help="Unlock match tolerances, weights, composite controls, and FX settings",
        )

    # ── Advanced settings grid (shown when toggled) ──
    if advanced_mode:
        st.divider()
        adv1, adv2, adv3 = st.columns(3, gap="large")

        with adv1:
            st.caption("Match Tolerances")
            date_tolerance = st.slider(
                "Date tolerance (days)", 0, 7, 3,
                help="Days of slack allowed between bank and book dates",
            )
            amount_tol_abs = st.number_input(
                "Amount rounding tolerance", value=0.01, step=0.01, format="%.2f",
                help="Absolute floor for rounding differences (e.g. 0.01 = 1 paise)",
            )

            st.caption("Part-Payment Matching")
            composite_match = st.toggle(
                "Enable composite (1-to-N / N-to-1)", value=True,
                help="Resolve split payments: one bank entry = multiple ledger items, or vice versa",
            )
            if composite_match:
                composite_max   = st.slider(
                    "Max items per group", 2, 8, 5,
                    help="Higher catches larger splits but is slower on big files",
                )
                composite_slack = st.slider(
                    "Date window (days)", 7, 60, 14,
                    help="Maximum date gap between part-payment entries",
                )

        with adv2:
            st.caption("Match Weights")
            ref_weight  = st.slider("Reference weight", 0.0, 1.0, 0.40, 0.05)
            amt_weight  = st.slider("Amount weight",    0.0, 1.0, 0.35, 0.05)
            date_weight = st.slider("Date weight",      0.0, 1.0, 0.25, 0.05)
            _tw = ref_weight + amt_weight + date_weight
            if _tw > 0:
                ref_weight  /= _tw
                amt_weight  /= _tw
                date_weight /= _tw

        with adv3:
            st.caption("Currency & FX")
            base_currency = st.selectbox(
                "Base currency",
                ['INR', 'USD', 'EUR', 'GBP', 'AED', 'SGD', 'AUD', 'JPY', 'Other'],
                index=0,
                help="All amounts are normalised to this currency for matching",
            )
            fx_rates_text = st.text_area(
                "FX Rates  (CCY=rate, one per line)",
                value="USD=84.50\nEUR=91.20\nGBP=107.80\nAED=23.00\nSGD=63.00\nAUD=55.00\nJPY=0.56",
                height=100,
                help="Applied only when your files contain a Currency column",
            )
            fx_rates = _parse_fx_rates(fx_rates_text)

        # ── Active config summary badge ──
        st.divider()
        comp_state = f"<span style='color:#34d399;'>On</span>" if composite_match else "<span style='color:#f87171;'>Off</span>"
        st.markdown(
            f"<div style='font-size:0.72rem;color:#475569;line-height:2;'>"
            f"<span style='color:#3B82F6;font-weight:700;font-size:0.62rem;"
            f"letter-spacing:0.08em;text-transform:uppercase;'>Active Config</span>"
            f" &nbsp;·&nbsp; "
            f"Date tol: <span style='color:#a5b4fc;'>{date_tolerance}d</span>"
            f" &nbsp;·&nbsp; "
            f"Base: <span style='color:#a5b4fc;'>{base_currency}</span>"
            f" &nbsp;·&nbsp; "
            f"Composite: {comp_state}"
            f"</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            "<div style='font-size:0.72rem;color:#334155;margin-top:0.5rem;"
            "display:flex;gap:1.5rem;flex-wrap:wrap;'>"
            "<span>✅ Smart defaults active</span>"
            "<span>✅ Composite matching on</span>"
            "<span>✅ All 4 match passes</span>"
            "<span>✅ Account matching auto</span>"
            "</div>",
            unsafe_allow_html=True,
        )


# ══════════════════════════════════════════════
#  FILE UPLOAD
# ══════════════════════════════════════════════
st.markdown('<div style="margin-top:1.5rem;"></div>', unsafe_allow_html=True)
st.markdown('<div class="section-pill">📂  Data Sources</div>', unsafe_allow_html=True)

u1, u2 = st.columns(2, gap="large")

with u1:
    st.markdown(
        '<div class="upload-label">🏦  Bank Statement</div>',
        unsafe_allow_html=True,
    )
    bank_file = st.file_uploader(
        "Bank statement", type=['csv', 'xlsx', 'xls'],
        key='bank', label_visibility='collapsed',
    )

with u2:
    st.markdown(
        '<div class="upload-label">📒  Ledger / Cashbook</div>',
        unsafe_allow_html=True,
    )
    ledger_file = st.file_uploader(
        "Ledger", type=['csv', 'xlsx', 'xls'],
        key='ledger', label_visibility='collapsed',
    )


# ══════════════════════════════════════════════
#  COLUMN MAPPER + RUN
# ══════════════════════════════════════════════
if bank_file and ledger_file:
    bank_df_raw   = load_file(bank_file)
    ledger_df_raw = load_file(ledger_file)

    # Stop early if either file could not be loaded
    if bank_df_raw is None or ledger_df_raw is None:
        st.stop()

    with st.expander("Preview — Bank Statement", expanded=False):
        st.dataframe(bank_df_raw, use_container_width=True)
    with st.expander("Preview — Ledger / Cashbook", expanded=False):
        st.dataframe(ledger_df_raw, use_container_width=True)

    st.markdown('<div style="margin:2rem 0 0.6rem;"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-pill">🗂️  Column Mapping</div>', unsafe_allow_html=True)
    st.markdown("""
    <div style="color:#475569;font-size:0.8rem;margin-bottom:1rem;line-height:1.6;">
        Map your file columns to the required fields. Fields marked
        <span style="color:#3B82F6;font-weight:600;">*</span> are required.
        Columns are auto-detected where possible — confirm before running.
    </div>""", unsafe_allow_html=True)

    m1, m2 = st.columns(2, gap="large")

    with m1:
        bank_mapping = _col_mapper_ui(bank_df_raw, "Bank Statement Columns", "bank")

    with m2:
        ledger_mapping = _col_mapper_ui(ledger_df_raw, "Ledger Columns", "ledger")

    st.markdown('<div style="margin:1.2rem 0;"></div>', unsafe_allow_html=True)

    # Validate: at least date + amount (or debit/credit) must be mapped
    def _mapping_valid(m):
        has_date   = bool(m.get('date'))
        has_amount = (bool(m.get('amount'))
                      or (bool(m.get('debit')) and bool(m.get('credit'))))
        return has_date and has_amount

    mapping_ok = _mapping_valid(bank_mapping) and _mapping_valid(ledger_mapping)
    if not mapping_ok:
        st.warning("Map at least Date and Amount (or Debit + Credit) for both files.")

    if st.button("Run Reconciliation", type="primary",
                 use_container_width=True, disabled=not mapping_ok):

        # ── Preprocess & validate columns ──
        bank_df   = _preprocess_df(bank_df_raw,   bank_mapping)
        ledger_df = _preprocess_df(ledger_df_raw, ledger_mapping)

        if bank_df is None or ledger_df is None:
            st.stop()

        config = MatchConfig(
            date_tolerance_days  = date_tolerance,
            amount_tolerance_abs = amount_tol_abs,
            reference_weight     = ref_weight,
            amount_weight        = amt_weight,
            date_weight          = date_weight,
            base_currency        = base_currency,
            fx_rates             = fx_rates,
            composite_match      = composite_match,
            composite_max_items  = composite_max,
            composite_date_slack = composite_slack,
        )

        try:
            with st.spinner("Reconciling transactions… please wait"):
                if len(bank_df) > 2000:
                    config.composite_match = False

                _wall0   = time.perf_counter()
                results  = reconcile(bank_df, ledger_df, config)
                _wall1   = time.perf_counter()
                month_end = generate_month_end_recon(results, opening_ledger_balance=opening_balance)
                _wall2   = time.perf_counter()
                print(f"[LedgerSync] app reconcile(): {_wall1 - _wall0:.3f}s  "
                      f"month_end: {_wall2 - _wall1:.3f}s  "
                      f"total: {_wall2 - _wall0:.3f}s")

            st.session_state['results']      = results
            st.session_state['month_end']    = month_end
            st.session_state['period_label'] = period_label
            st.session_state['opening_bal']  = opening_balance
            st.success("Reconciliation complete.")

            # ── Post-run warnings ──
            s = results['summary']
            unmatched_pct = (s['unmatched_bank'] / max(s['total_bank'], 1)) * 100
            if unmatched_pct > 30:
                st.warning(
                    f"{s['unmatched_bank']:,} bank transactions ({unmatched_pct:.0f}%) could not be matched. "
                    "Check column mappings, date formats, and whether the correct files were uploaded."
                )
            if s['unmatched_ledger'] > 50:
                st.warning(
                    f"{s['unmatched_ledger']:,} ledger entries have no matching bank transaction. "
                    "These may represent outstanding payments or data entry errors."
                )
            if not month_end['recon_table'].empty:
                dup_count = (month_end['recon_table'].get('status', pd.Series()) == 'ERROR_DUPLICATE').sum()
                if dup_count > 0:
                    st.warning(
                        f"{dup_count} possible duplicate transaction(s) detected. "
                        "Review the 'Month-End View' tab for details."
                    )

        except Exception:
            st.error(
                "Error processing file. Please verify format and mappings. "
                "If the problem persists, check that date columns contain valid dates "
                "and amount columns contain numbers only."
            )
            st.stop()


# ══════════════════════════════════════════════
#  RESULTS
# ══════════════════════════════════════════════
if 'results' in st.session_state:
    results     = st.session_state['results']
    month_end   = st.session_state['month_end']
    period_lbl  = st.session_state.get('period_label', 'Current Period')
    opening_bal = st.session_state.get('opening_bal', 0.0)
    summary     = results['summary']
    recon_table = month_end['recon_table']
    carry_fwd   = month_end['carry_forward']
    ccy         = summary.get('base_currency', 'INR')
    sym         = '₹' if ccy == 'INR' else f'{ccy} '

    total_txns = (summary['exact_matches'] + summary['near_matches']
                  + summary['composite_matches'] + summary['unmatched_bank'])
    rate       = summary['match_rate']
    exceptions = summary['unmatched_bank'] + summary['unmatched_ledger']
    diff       = summary['difference']
    composites = summary['composite_matches']

    # ── KPI Cards ──
    st.markdown('<div style="margin:2rem 0 0.5rem;"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-pill">📊  Reconciliation Summary</div>', unsafe_allow_html=True)

    rate_color = "#34d399" if rate >= 85 else "#fbbf24" if rate >= 60 else "#f87171"
    exc_color  = "#34d399" if exceptions == 0 else "#f87171"
    diff_color = "#34d399" if abs(diff) < 0.01 else "#f87171"

    k1, k2, k3, k4 = st.columns(4, gap="medium")

    with k1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Total Transactions</div>
            <div class="kpi-value">{total_txns:,}</div>
            <div class="kpi-sub">Bank entries processed</div>
        </div>""", unsafe_allow_html=True)

    with k2:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Match Rate</div>
            <div class="kpi-value" style="background:linear-gradient(135deg,{rate_color},#a5b4fc);
                -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">
                {rate}%
            </div>
            <div class="kpi-sub">All 4 passes combined</div>
        </div>""", unsafe_allow_html=True)

    with k3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Needs Review</div>
            <div class="kpi-value" style="background:linear-gradient(135deg,{exc_color},#a5b4fc);
                -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">
                {exceptions:,}
            </div>
            <div class="kpi-sub">Unmatched items remaining</div>
        </div>""", unsafe_allow_html=True)

    with k4:
        diff_fmt = f"{sym}{abs(diff):,.2f}"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-label">Difference ({ccy})</div>
            <div class="kpi-value" style="background:linear-gradient(135deg,{diff_color},#a5b4fc);
                -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;">
                {diff_fmt}
            </div>
            <div class="kpi-sub">Bank vs Ledger</div>
        </div>""", unsafe_allow_html=True)

    # ── Balances row ──
    st.markdown('<div style="margin:1.8rem 0 0.5rem;"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-pill">💰  Balances</div>', unsafe_allow_html=True)

    t1, t2, t3 = st.columns(3, gap="medium")
    with t1:
        st.markdown(f"""
        <div class="kpi-card" style="padding:1rem 1.5rem;">
            <div class="kpi-label">Bank Total ({ccy})</div>
            <div style="font-size:1.4rem;font-weight:600;color:#e2e8f0;">{sym}{summary['bank_total']:,.2f}</div>
        </div>""", unsafe_allow_html=True)
    with t2:
        st.markdown(f"""
        <div class="kpi-card" style="padding:1rem 1.5rem;">
            <div class="kpi-label">Ledger Total ({ccy})</div>
            <div style="font-size:1.4rem;font-weight:600;color:#e2e8f0;">{sym}{summary['ledger_total']:,.2f}</div>
        </div>""", unsafe_allow_html=True)
    with t3:
        dc    = "#34d399" if abs(diff) < 0.01 else "#f87171"
        dsign = "+" if diff > 0 else ""
        st.markdown(f"""
        <div class="kpi-card" style="padding:1rem 1.5rem;">
            <div class="kpi-label">Net Difference ({ccy})</div>
            <div style="font-size:1.4rem;font-weight:600;color:{dc};">{dsign}{sym}{diff:,.2f}</div>
        </div>""", unsafe_allow_html=True)

    # ══════════════════════════════════════════
    #  TABS
    # ══════════════════════════════════════════
    st.markdown('<div style="margin:2rem 0 0.5rem;"></div>', unsafe_allow_html=True)
    st.markdown('<div class="section-pill">📋  Detail</div>', unsafe_allow_html=True)
    st.markdown('<div style="margin-bottom:0.75rem;"></div>', unsafe_allow_html=True)

    tab_ov, tab_brs, tab_matched, tab_near, tab_comp, tab_me, tab_exc, tab_exp = st.tabs([
        "Overview",
        "BRS Statement",
        f"Matched  ({summary['exact_matches']})",
        f"Possible Matches  ({summary['near_matches']})",
        f"Split Matches  ({composites})",
        "Month-End View",
        f"Needs Review  ({exceptions})",
        "Export",
    ])

    # ── Overview ─────────────────────────────────────────────────────────
    with tab_ov:
        ov1, ov2 = st.columns(2, gap="large")
        with ov1:
            st.markdown(f"""
            <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(59,130,246,0.2);
                        border-radius:12px;padding:1.2rem;">
                <div class="kpi-label" style="margin-bottom:0.8rem;">Match Breakdown</div>
                <div class="brs-row">
                    <span style="color:#94a3b8;font-size:0.85rem;">Exact Matches (Pass 1)</span>
                    <span class="badge-matched">{summary['exact_matches']}</span>
                </div>
                <div class="brs-row">
                    <span style="color:#94a3b8;font-size:0.85rem;">Possible Matches (Pass 2)</span>
                    <span class="badge-review">{summary['near_matches']}</span>
                </div>
                <div class="brs-row">
                    <span style="color:#94a3b8;font-size:0.85rem;">Split Matches (Pass 3+4)</span>
                    <span class="badge-composite">{composites}</span>
                </div>
                <div class="brs-row">
                    <span style="color:#94a3b8;font-size:0.85rem;">Unmatched Bank</span>
                    <span class="badge-exception">{summary['unmatched_bank']}</span>
                </div>
                <div class="brs-row" style="border-bottom:none;">
                    <span style="color:#94a3b8;font-size:0.85rem;">Unmatched Ledger</span>
                    <span class="badge-exception">{summary['unmatched_ledger']}</span>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with ov2:
            bar_w = int(rate)
            feat_rows = ""
            if summary.get('has_currency'):
                feat_rows += f'<div style="font-size:0.75rem;color:#a5b4fc;margin-bottom:0.3rem;">Multi-currency active · Base: {ccy}</div>'
            if summary.get('has_account'):
                feat_rows += '<div style="font-size:0.75rem;color:#a5b4fc;margin-bottom:0.3rem;">Account-number matching active</div>'
            if composites > 0:
                feat_rows += f'<div style="font-size:0.75rem;color:#c084fc;margin-bottom:0.3rem;">{composites} split / part-payment group(s) found</div>'

            st.markdown(f"""
            <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(59,130,246,0.2);
                        border-radius:12px;padding:1.2rem;">
                <div class="kpi-label" style="margin-bottom:1rem;">Match Rate</div>
                <div style="font-size:2.5rem;font-weight:700;
                            background:linear-gradient(135deg,#34d399,#3B82F6);
                            -webkit-background-clip:text;-webkit-text-fill-color:transparent;
                            background-clip:text;margin-bottom:0.8rem;">{rate}%</div>
                <div style="background:rgba(15,23,42,0.8);border-radius:100px;height:6px;overflow:hidden;">
                    <div style="width:{bar_w}%;height:100%;background:linear-gradient(90deg,#3B82F6,#1D4ED8);border-radius:100px;"></div>
                </div>
                <div style="font-size:0.72rem;color:#475569;margin-top:0.5rem;">
                    {summary['exact_matches'] + summary['near_matches'] + composites} of {total_txns} bank entries matched
                </div>
                <div style="margin-top:0.8rem;">{feat_rows}</div>
            </div>
            """, unsafe_allow_html=True)

        # Status distribution
        if not recon_table.empty and 'status' in recon_table.columns:
            st.markdown('<div style="margin-top:1.2rem;"></div>', unsafe_allow_html=True)
            st.markdown('<div class="kpi-label">Status Distribution</div>', unsafe_allow_html=True)
            status_counts = recon_table['status'].value_counts()
            STATUS_COLORS = {
                'CLEARED':              '#34d399',
                'COMPOSITE_CLEARED':    '#c084fc',
                'LIKELY_CLEARED':       '#6ee7b7',
                'REVIEW':               '#fbbf24',
                'ADJUSTMENT_REQUIRED':  '#f97316',
                'UNRECORDED_RECEIPT':   '#a5b4fc',
                'PAYMENT_NOT_PROCESSED':'#fb7185',
                'REVERSAL_ENTRY':       '#c084fc',
                'ERROR_DUPLICATE':      '#f87171',
            }
            s_cols = st.columns(min(len(status_counts), 5), gap="small")
            for i, (status, cnt) in enumerate(status_counts.items()):
                col = STATUS_COLORS.get(status, '#94a3b8')
                with s_cols[i % 5]:
                    st.markdown(f"""
                    <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(59,130,246,0.18);
                                border-radius:10px;padding:0.7rem 0.8rem;text-align:center;margin-bottom:0.5rem;">
                        <div style="font-size:1.3rem;font-weight:700;color:{col};">{cnt}</div>
                        <div style="font-size:0.6rem;color:#475569;margin-top:0.2rem;letter-spacing:0.04em;">
                            {status.replace('_',' ')}
                        </div>
                    </div>""", unsafe_allow_html=True)

    # ── BRS Statement ────────────────────────────────────────────────────
    with tab_brs:
        st.markdown('<div style="margin:0.75rem 0 1.2rem;"></div>', unsafe_allow_html=True)
        _render_brs(summary, results, period_lbl, opening_bal)

        if not carry_fwd.empty:
            st.markdown('<div style="margin-top:2rem;"></div>', unsafe_allow_html=True)
            st.markdown('<div class="section-pill">⏩  Carry-Forward Items</div>', unsafe_allow_html=True)
            st.markdown(f"""
            <div style="color:#64748b;font-size:0.8rem;margin:0.5rem 0 0.8rem;">
                {len(carry_fwd)} items carry forward to next period (all non-cleared statuses).
            </div>""", unsafe_allow_html=True)
            cf_cols = [c for c in ['carry_forward_reason','date','amount',
                                   'reference','description','remarks'] if c in carry_fwd.columns]
            st.dataframe(safe_df(carry_fwd[cf_cols]), use_container_width=True, height=300)

    # ── Matched ──────────────────────────────────────────────────────────
    with tab_matched:
        if not results['matched'].empty:
            st.dataframe(safe_df(results['matched']), use_container_width=True, height=440)
        else:
            st.info("No exact matches found.")

    # ── Near Match ───────────────────────────────────────────────────────
    with tab_near:
        if not results['near_matched'].empty:
            st.dataframe(
                safe_df(results['near_matched']).style.map(
                    lambda v: ('background-color:rgba(245,158,11,0.15);color:#fbbf24'
                               if isinstance(v, float) and 0.5 <= v < 0.85 else ''),
                    subset=['confidence'],
                ),
                use_container_width=True, height=440,
            )
        else:
            st.info("No possible matches found.")

    # ── Composite ────────────────────────────────────────────────────────
    with tab_comp:
        composite_df = results.get('composite', pd.DataFrame())
        if not composite_df.empty:
            st.markdown("""
            <div style="color:#64748b;font-size:0.8rem;margin-bottom:1rem;">
                <strong style="color:#c084fc;">Split matches</strong> resolve
                part-payments and split transactions.
                The <em>bank side</em> may show multiple dates/refs (N-to-1),
                or the <em>ledger/ERP side</em> may show multiple refs (1-to-N).<br>
                Confidence is fixed at 75% — review each group before confirming.
            </div>""", unsafe_allow_html=True)

            # Group display
            by_type = composite_df.groupby('match_type')
            for mtype, grp in by_type:
                n = len(grp)
                badge_col = "#c084fc"
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:0.6rem;margin:0.8rem 0 0.3rem;">
                    <span style="background:rgba(139,92,246,0.15);border:1px solid rgba(139,92,246,0.35);
                                 color:{badge_col};border-radius:6px;padding:0.1rem 0.55rem;
                                 font-size:0.72rem;font-weight:600;">{mtype}</span>
                    <span style="color:#475569;font-size:0.8rem;">{n} group(s)</span>
                </div>""", unsafe_allow_html=True)

            st.dataframe(safe_df(composite_df), use_container_width=True, height=440)

            # Composite amount summary
            try:
                comp_bank_total = pd.to_numeric(composite_df['bank_amount'], errors='coerce').sum()
                comp_ledger_total = pd.to_numeric(composite_df['ledger_amount'], errors='coerce').sum()
                st.markdown(f"""
                <div style="margin-top:0.8rem;padding:0.8rem 1rem;
                            background:rgba(139,92,246,0.08);
                            border:1px solid rgba(139,92,246,0.2);
                            border-radius:10px;font-size:0.82rem;color:#94a3b8;">
                    Composite Bank Total: <strong style="color:#c084fc;">{sym}{comp_bank_total:,.2f}</strong>
                    &nbsp;·&nbsp;
                    Composite Ledger Total: <strong style="color:#c084fc;">{sym}{comp_ledger_total:,.2f}</strong>
                </div>""", unsafe_allow_html=True)
            except Exception:
                pass
        else:
            st.markdown("""
            <div style="text-align:center;padding:2.5rem;
                        background:rgba(30,41,59,0.4);
                        border:1px dashed rgba(139,92,246,0.25);
                        border-radius:12px;color:#475569;">
                <div style="font-size:1.5rem;margin-bottom:0.5rem;opacity:0.5;">⬡</div>
                No split / part-payment matches found.<br>
                <span style="font-size:0.75rem;">
                  All transactions reconcile 1-to-1. Enable split matching
                  in Settings (Advanced Mode) if you expect part-payments.
                </span>
            </div>""", unsafe_allow_html=True)

    # ── Month-End View ───────────────────────────────────────────────────
    with tab_me:
        st.markdown("""
        <div style="color:#64748b;font-size:0.8rem;margin-bottom:1rem;">
            Full audit-ready reconciliation table — status classification, aging,
            remarks, and suggested journal entries from the month-end engine.
        </div>""", unsafe_allow_html=True)

        if not recon_table.empty:
            STATUS_COLORS = {
                'CLEARED':              'color:#34d399;font-weight:600',
                'COMPOSITE_CLEARED':    'color:#c084fc;font-weight:600',
                'LIKELY_CLEARED':       'color:#6ee7b7;font-weight:600',
                'REVIEW':               'color:#fbbf24;font-weight:600',
                'ADJUSTMENT_REQUIRED':  'color:#f97316;font-weight:600',
                'UNRECORDED_RECEIPT':   'color:#a5b4fc;font-weight:600',
                'PAYMENT_NOT_PROCESSED':'color:#fb7185;font-weight:600',
                'REVERSAL_ENTRY':       'color:#c084fc;font-weight:600',
                'ERROR_DUPLICATE':      'color:#f87171;font-weight:600',
            }
            display_cols = [c for c in [
                'status','category','date','amount','reference','description',
                'aging_bucket','aging_days','remarks','suggested_entry',
            ] if c in recon_table.columns]

            styled = safe_df(recon_table[display_cols]).style.map(
                lambda v: STATUS_COLORS.get(v, ''), subset=['status']
            )
            st.dataframe(styled, use_container_width=True, height=500)

            # Aging buckets
            if 'aging_bucket' in recon_table.columns:
                st.markdown('<div style="margin-top:1.2rem;"></div>', unsafe_allow_html=True)
                aging_counts = recon_table['aging_bucket'].value_counts()
                AGING_COLORS = {'CURRENT':'#34d399','SHORT_DELAY':'#fbbf24',
                                'OLD':'#f87171','UNKNOWN':'#475569'}
                ag_cols = st.columns(len(aging_counts), gap="small")
                for i, (bucket, cnt) in enumerate(aging_counts.items()):
                    color = AGING_COLORS.get(bucket, '#94a3b8')
                    with ag_cols[i]:
                        st.markdown(f"""
                        <div style="background:rgba(30,41,59,0.5);border:1px solid rgba(59,130,246,0.18);
                                    border-radius:10px;padding:0.7rem;text-align:center;">
                            <div style="font-size:1.3rem;font-weight:700;color:{color};">{cnt}</div>
                            <div style="font-size:0.65rem;color:#475569;margin-top:0.2rem;">
                                {bucket.replace('_',' ')}
                            </div>
                        </div>""", unsafe_allow_html=True)

    # ── Exceptions ───────────────────────────────────────────────────────
    with tab_exc:
        ex1, ex2 = st.columns(2, gap="large")
        with ex1:
            st.markdown('<div style="color:#f87171;font-size:0.78rem;font-weight:600;'
                        'letter-spacing:0.06em;margin-bottom:0.4rem;">UNMATCHED — BANK STATEMENT</div>',
                        unsafe_allow_html=True)
            if not results['unmatched_bank'].empty:
                st.dataframe(safe_df(results['unmatched_bank']), use_container_width=True, height=400)
            else:
                st.success("All bank transactions matched.")

        with ex2:
            st.markdown('<div style="color:#f87171;font-size:0.78rem;font-weight:600;'
                        'letter-spacing:0.06em;margin-bottom:0.4rem;">UNMATCHED — LEDGER / ERP</div>',
                        unsafe_allow_html=True)
            if not results['unmatched_ledger'].empty:
                st.dataframe(safe_df(results['unmatched_ledger']), use_container_width=True, height=400)
            else:
                st.success("All ledger entries matched.")

    # ── Export ───────────────────────────────────────────────────────────
    with tab_exp:
        st.markdown("""
        <div style="padding:1rem 0;color:#94a3b8;font-size:0.85rem;">
            8-sheet Excel workbook: BRS Statement, Month-End View, Carry Forward,
            Exact Matches, Possible Matches, Split Matches, Unmatched Bank, Unmatched Ledger.
        </div>""", unsafe_allow_html=True)

        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # BRS summary
            unmatched_bank_amt_ex   = (results['unmatched_bank']['amount'].sum()
                                       if not results['unmatched_bank'].empty else 0)
            unmatched_ledger_amt_ex = (results['unmatched_ledger']['amount'].sum()
                                       if not results['unmatched_ledger'].empty else 0)
            brs_df = pd.DataFrame({
                'Particulars': [
                    'Opening Ledger Balance (provided)',
                    'Balance as per Bank Statement',
                    f'Add: Outstanding Ledger Entries ({summary["unmatched_ledger"]} items)',
                    f'Less: Unrecorded Bank Entries ({summary["unmatched_bank"]} items)',
                    'Adjusted Bank Balance',
                    'Balance as per Books (Ledger)',
                    'Difference',
                ],
                f'Amount ({ccy})': [
                    opening_bal,
                    summary['bank_total'],
                    unmatched_ledger_amt_ex,
                    unmatched_bank_amt_ex,
                    summary['bank_total'] + unmatched_ledger_amt_ex - unmatched_bank_amt_ex,
                    summary['ledger_total'],
                    summary['difference'],
                ],
            })
            brs_df.to_excel(writer, sheet_name='BRS Statement', index=False)
            recon_table.to_excel(writer, sheet_name='Month-End View', index=False)
            carry_fwd.to_excel(writer, sheet_name='Carry Forward', index=False)
            results['matched'].to_excel(writer, sheet_name='Exact Matches', index=False)
            results['near_matched'].to_excel(writer, sheet_name='Possible Matches', index=False)
            composite_df_ex = results.get('composite', pd.DataFrame())
            composite_df_ex.to_excel(writer, sheet_name='Split Matches', index=False)
            results['unmatched_bank'].to_excel(writer, sheet_name='Unmatched Bank', index=False)
            results['unmatched_ledger'].to_excel(writer, sheet_name='Unmatched Ledger', index=False)

        dl1, _, _ = st.columns([1, 1, 1])
        with dl1:
            st.download_button(
                label="Download Full Report (.xlsx)",
                data=buffer.getvalue(),
                file_name=f"ledgersync_{period_lbl.replace(' ','_').lower()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

else:
    st.markdown(
        '<div style="text-align:center;padding:4rem 2rem;margin-top:2rem;'
        'background:rgba(17,24,39,0.55);border:1.5px dashed rgba(59,130,246,0.2);'
        'border-radius:20px;backdrop-filter:blur(10px);">'
        '<div style="font-size:2.8rem;margin-bottom:1rem;'
        'filter:drop-shadow(0 0 12px rgba(99,102,241,0.35));">⬡</div>'
        '<div style="font-size:1.05rem;font-weight:600;color:#94a3b8;margin-bottom:0.5rem;">'
        'Ready to reconcile</div>'
        '<div style="font-size:0.82rem;color:#475569;line-height:1.7;max-width:400px;margin:0 auto;">'
        'Upload your <span style="color:#a5b4fc;font-weight:600;">Bank Statement</span>'
        ' and <span style="color:#a5b4fc;font-weight:600;">Ledger / Cashbook</span> above,'
        ' then click <strong style="color:#3B82F6;">Run Reconciliation</strong> to begin.'
        '</div>'
        '<div style="display:flex;justify-content:center;gap:2rem;margin-top:1.8rem;'
        'font-size:0.72rem;color:#334155;font-weight:600;letter-spacing:0.04em;">'
        '<span>✦ 4-Pass Matching</span>'
        '<span>✦ BRS Statement</span>'
        '<span>✦ Month-End View</span>'
        '<span>✦ Excel Export</span>'
        '</div>'
        '</div>',
        unsafe_allow_html=True,
    )


# ══════════════════════════════════════════════
#  FOOTER
# ══════════════════════════════════════════════
st.markdown("""
<div class="xbp-footer">
    LedgerSync &nbsp;·&nbsp; <span>by R@k</span>
</div>
""", unsafe_allow_html=True)
