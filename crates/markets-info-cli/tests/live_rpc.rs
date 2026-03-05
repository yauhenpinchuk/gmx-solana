//! Optional integration tests that require RPC and a real Market PDA.
//! Run with: RPC_URL=... TEST_MARKET_PDA=... cargo test -p gmsol-markets-info-cli live_ -- --ignored

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gmsol-markets-info-cli"))
}

#[test]
#[ignore = "requires RPC_URL and TEST_MARKET_PDA env vars"]
fn single_market_returns_valid_json() {
    let rpc_url = std::env::var("RPC_URL").unwrap_or_else(|_| String::new());
    let pda = std::env::var("TEST_MARKET_PDA").unwrap_or_else(|_| String::new());
    if rpc_url.is_empty() || pda.is_empty() {
        eprintln!("Skip: set RPC_URL and TEST_MARKET_PDA to run");
        return;
    }
    let out = bin()
        .args(["--rpc-url", &rpc_url, "--market-pdas", &pda])
        .output()
        .unwrap();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).expect("valid JSON");
    assert!(json.get("funding_factor_per_second").is_some());
    assert!(json.get("borrowing_factor_per_second_for_long").is_some());
    assert!(json.get("open_interest_long_usd").is_some());
}
