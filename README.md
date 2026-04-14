# 🏦 Bank Reconciliation Tool

Auto-match bank transactions against your ledger/cashbook using multiple parameters.

## Quick Start

```bash
# 1. Create virtual environment
python -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the app
streamlit run app.py
```

## How It Works

Upload two files (CSV or Excel):
- **Bank Statement** — with columns: date, amount, reference, narration
- **Ledger/Cashbook** — with columns: date, amount, voucher_no, party_name

The engine matches transactions in two passes:

| Pass | Type | Logic | Confidence |
|------|------|-------|------------|
| 1 | Exact | Amount + Reference + Date all align | ≥ 0.85 |
| 2 | Near | Amount matches, date within tolerance, weaker ref | 0.50 – 0.84 |

Unmatched items from both sides are flagged for manual review.

## Sample Data

Use `sample_bank.csv` and `sample_ledger.csv` to test. They include:
- Exact matches (same ref, amount, date)
- Date mismatches (bank clears 2 days later)
- Reference mismatches (different ref formats)
- Unmatched entries (ledger has entries not in bank)

## Project Structure

```
bank-recon/
├── app.py              # Streamlit UI
├── matcher.py          # Core matching engine
├── sample_bank.csv     # Test bank statement
├── sample_ledger.csv   # Test ledger
├── requirements.txt
└── README.md
```

## Roadmap

- [x] Phase 1: Core matching (exact + near)
- [ ] Phase 2: Fuzzy name matching + one-to-many
- [ ] Phase 3: PDF bank statement parsing
- [ ] Phase 4: AI-powered match suggestions & narratives
