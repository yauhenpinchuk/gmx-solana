# gmsol-markets-info-cli

Read a GMX-Solana Market account and print **funding**, **borrowing**, and **open interest** as JSON. Single source of truth for on-chain rates.

## Build

```bash
cargo build --release -p gmsol-markets-info-cli
# Binary: target/release/gmsol-markets-info-cli
```

## Tests (for CI/CD)

```bash
cargo test -p gmsol-markets-info-cli
```

Runs unit tests (parse_market edge cases) and integration tests (CLI args: --help, missing/conflicting args, invalid PDA). Optional live RPC test is ignored by default; run with `RPC_URL=... TEST_MARKET_PDA=... cargo test -p gmsol-markets-info-cli live_ -- --ignored` to test against a real endpoint.

## Usage

One flag: **--market-pdas**. One PDA → single JSON object; several PDAs (comma-separated) → JSON array (one `getMultipleAccounts` per 100 PDAs).

**Single market:**
```bash
gmsol-markets-info-cli --rpc-url <RPC_URL> --market-pdas <MARKET_PDA>
```

**Batch (many markets):**
```bash
gmsol-markets-info-cli --rpc-url <RPC_URL> --market-pdas <PDA1>,<PDA2>,...
```

- **--rpc-url** — Solana RPC (e.g. `https://api.mainnet-beta.solana.com`).
- **--market-pdas** — One Market PDA or comma-separated list; batch fetches up to 100 per RPC call.

## Output (JSON, single line)

**Single:** one object. **Batch:** JSON array of objects; each object includes `"market_pda"` so you can match results.

```json
{"funding_factor_per_second":2351289366068,"borrowing_factor_per_second_for_long":134454569266,"borrowing_factor_per_second_for_short":0,"open_interest_long_usd":1234567890123456789,"open_interest_short_usd":9876543210987654321}
```

All numeric values use **1e20** fixed-point. In Python: `% per hour (borrowing) = value / 1e20 * 3600 * 100`.

## Integration

- Set `GMSOL_MARKETS_INFO_CLI` to this binary (or leave unset to use `../gmx-solana/target/release/gmsol-markets-info-cli`).
- Set `RPC_URL`. Single: `get_onchain_market_status(market_pda)` (CLI called with `--market-pdas` and one PDA). Batch: `get_onchain_market_status_batch(list_of_pdas)` — one CLI call and one getMultipleAccounts for all.
