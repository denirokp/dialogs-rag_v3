import duckdb, json, sys

def fail(msg):
    print(f"❌ {msg}")
    sys.exit(1)

con = duckdb.connect(database=':memory:')

# Expect parquet/csv or tables already loaded by outer runner
# For simplicity assume tables 'mentions','utterances','dialogs' already exist.

with open('quality/checks.sql', 'r', encoding='utf-8') as f:
    queries = [q.strip() for q in f.read().split(';') if q.strip()]

# Run individual named checks
empty_quotes = con.execute(queries[0]).fetchone()[0]
non_client = con.execute(queries[1]).fetchone()[0]
dup_pct = con.execute(queries[2]).fetchone()[0]
misc_share = con.execute(queries[3]).fetchone()[0]

print(f"Evidence-100 empty_quotes={empty_quotes}")
print(f"Client-only-100 non_client_mentions={non_client}")
print(f"Dedup dup_pct={dup_pct}%")
print(f"Coverage misc_share_pct={misc_share}%")

if empty_quotes != 0: fail("Evidence-100 violated (empty quotes found)")
if non_client != 0: fail("Client-only-100 violated (non-client mentions found)")
if dup_pct > 1.0: fail("Dedup ≤1% violated")
if misc_share > 2.0: fail("Coverage ≥98% violated")

# Ambiguity report (informational)
amb = con.execute(queries[4]).fetchdf()
print("\nAmbiguity (confidence<0.6) by subtheme:")
print(amb.to_string(index=False))

print("\n✅ DoD checks passed.")
