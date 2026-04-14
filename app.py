"""
Bank Reconciliation Tool - Phase 1
Run: streamlit run app.py
"""

import streamlit as st
import pandas as pd
from matcher import reconcile, MatchConfig

# ── Page Config ──
st.set_page_config(page_title="Bank Reconciliation Tool", page_icon="🏦", layout="wide")

st.title("🏦 Bank Reconciliation Tool")
st.caption("Upload your bank statement and ledger to auto-match transactions.")

# ── Sidebar: Settings ──
with st.sidebar:
    st.header("⚙️ Match Settings")
    date_tolerance = st.slider("Date tolerance (days)", 0, 7, 3,
                               help="How many days of difference to allow between bank and book dates")
    st.divider()
    st.markdown("**Match Weights**")
    ref_weight = st.slider("Reference weight", 0.0, 1.0, 0.4, 0.05)
    amt_weight = st.slider("Amount weight", 0.0, 1.0, 0.35, 0.05)
    date_weight = st.slider("Date weight", 0.0, 1.0, 0.25, 0.05)

    # Normalize weights to sum to 1
    total_w = ref_weight + amt_weight + date_weight
    if total_w > 0:
        ref_weight /= total_w
        amt_weight /= total_w
        date_weight /= total_w

    st.divider()
    st.markdown("**Phase 1** — Exact & Near matching")
    st.markdown("_Phase 2: Fuzzy names, 1-to-many_")
    st.markdown("_Phase 3: PDF parsing_")
    st.markdown("_Phase 4: AI explanations_")

# ── File Upload ──
col1, col2 = st.columns(2)

with col1:
    st.subheader("📄 Bank Statement")
    bank_file = st.file_uploader("Upload bank statement", type=['csv', 'xlsx', 'xls'],
                                  key='bank', label_visibility='collapsed')

with col2:
    st.subheader("📒 Ledger / Cashbook")
    ledger_file = st.file_uploader("Upload ledger or cashbook", type=['csv', 'xlsx', 'xls'],
                                    key='ledger', label_visibility='collapsed')


def load_file(uploaded_file):
    """Load CSV or Excel file into DataFrame."""
    if uploaded_file.name.endswith('.csv'):
        return pd.read_csv(uploaded_file)
    else:
        return pd.read_excel(uploaded_file)


def safe_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalise a DataFrame for safe Streamlit/Arrow rendering.
    - Resets non-default index so Arrow doesn't choke on index gaps.
    - Converts any extension-array string dtype to plain object dtype.
    """
    df = df.reset_index(drop=True)
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) and pd.api.types.is_extension_array_dtype(df[col]):
            df[col] = df[col].astype(object)
    return df


# ── Main Logic ──
if bank_file and ledger_file:
    bank_df = load_file(bank_file)
    ledger_df = load_file(ledger_file)

    # Preview uploaded data
    with st.expander("👀 Preview: Bank Statement", expanded=False):
        st.dataframe(bank_df, use_container_width=True)

    with st.expander("👀 Preview: Ledger", expanded=False):
        st.dataframe(ledger_df, use_container_width=True)

    st.divider()

    # Run reconciliation
    if st.button("🔍 Run Reconciliation", type="primary", use_container_width=True):
        config = MatchConfig(
            date_tolerance_days=date_tolerance,
            reference_weight=ref_weight,
            amount_weight=amt_weight,
            date_weight=date_weight,
        )

        with st.spinner("Matching transactions..."):
            results = reconcile(bank_df, ledger_df, config)

        # Store in session state so results persist
        st.session_state['results'] = results

# ── Display Results ──
if 'results' in st.session_state:
    results = st.session_state['results']
    summary = results['summary']

    st.divider()
    st.header("📊 Reconciliation Results")

    # Summary metrics
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Exact Matches", summary['exact_matches'])
    m2.metric("Near Matches", summary['near_matches'])
    m3.metric("Unmatched (Bank)", summary['unmatched_bank'])
    m4.metric("Match Rate", f"{summary['match_rate']}%")

    # Totals
    t1, t2, t3 = st.columns(3)
    t1.metric("Bank Total", f"₹{summary['bank_total']:,.2f}")
    t2.metric("Ledger Total", f"₹{summary['ledger_total']:,.2f}")
    t3.metric("Difference", f"₹{summary['difference']:,.2f}",
              delta_color="inverse" if summary['difference'] != 0 else "off")

    st.divider()

    # Tabbed results
    tab1, tab2, tab3, tab4 = st.tabs([
        f"✅ Exact Matches ({summary['exact_matches']})",
        f"🟡 Near Matches ({summary['near_matches']})",
        f"❌ Unmatched Bank ({summary['unmatched_bank']})",
        f"❌ Unmatched Ledger ({summary['unmatched_ledger']})",
    ])

    with tab1:
        if not results['matched'].empty:
            st.dataframe(safe_df(results['matched']), use_container_width=True)
        else:
            st.info("No exact matches found.")

    with tab2:
        if not results['near_matched'].empty:
            st.dataframe(
                safe_df(results['near_matched']).style.map(
                    lambda v: 'background-color: #fff3cd' if isinstance(v, float) and 0.5 <= v < 0.85 else '',
                    subset=['confidence']
                ),
                use_container_width=True,
            )
        else:
            st.info("No near matches found.")

    with tab3:
        if not results['unmatched_bank'].empty:
            st.dataframe(safe_df(results['unmatched_bank']), use_container_width=True)
        else:
            st.success("All bank transactions matched!")

    with tab4:
        if not results['unmatched_ledger'].empty:
            st.dataframe(safe_df(results['unmatched_ledger']), use_container_width=True)
        else:
            st.success("All ledger entries matched!")

    # ── Export ──
    st.divider()
    st.subheader("📥 Export Results")

    export_col1, export_col2 = st.columns(2)

    with export_col1:
        # Combine all results into one Excel with multiple sheets
        import io
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            results['matched'].to_excel(writer, sheet_name='Exact Matches', index=False)
            results['near_matched'].to_excel(writer, sheet_name='Near Matches', index=False)
            results['unmatched_bank'].to_excel(writer, sheet_name='Unmatched Bank', index=False)
            results['unmatched_ledger'].to_excel(writer, sheet_name='Unmatched Ledger', index=False)

            # Summary sheet
            summary_df = pd.DataFrame([summary])
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

        st.download_button(
            label="⬇️ Download Full Report (.xlsx)",
            data=buffer.getvalue(),
            file_name="reconciliation_report.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

else:
    st.info("👆 Upload both files and click **Run Reconciliation** to start.")
