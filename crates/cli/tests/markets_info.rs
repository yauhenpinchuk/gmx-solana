//! Integration tests for the `gmsol markets-info` subcommand.
//! Verifies the `gmsol markets-info` subcommand contract (flags, output format).

use std::process::Command;

fn gmsol() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gmsol"))
}

#[test]
fn markets_info_help_shows_expected_flags() {
    let out = gmsol()
        .args(["markets-info", "--help"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("--rpc-url"), "missing --rpc-url in help");
    assert!(
        stdout.contains("--market-pdas"),
        "missing --market-pdas in help"
    );
}

#[test]
fn markets_info_no_args_fails() {
    let out = gmsol().arg("markets-info").output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("required") || stderr.contains("Usage"),
        "stderr: {stderr}"
    );
}

#[test]
fn markets_info_rpc_url_only_fails() {
    let out = gmsol()
        .args([
            "markets-info",
            "--rpc-url",
            "https://api.mainnet-beta.solana.com",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn markets_info_empty_pdas_fails() {
    let out = gmsol()
        .args([
            "markets-info",
            "--rpc-url",
            "https://api.mainnet-beta.solana.com",
            "--market-pdas",
            "",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn markets_info_only_commas_fails() {
    let out = gmsol()
        .args([
            "markets-info",
            "--rpc-url",
            "https://api.mainnet-beta.solana.com",
            "--market-pdas",
            ", ,",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn markets_info_invalid_pda_fails() {
    let out = gmsol()
        .args([
            "markets-info",
            "--rpc-url",
            "https://api.mainnet-beta.solana.com",
            "--market-pdas",
            "not-a-valid-pubkey",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn markets_info_invalid_pda_in_batch_fails() {
    let out = gmsol()
        .args([
            "markets-info",
            "--rpc-url",
            "https://api.mainnet-beta.solana.com",
            "--market-pdas",
            "CJg17Dn4xgUyEW3gKSSyteNw7LhP1o9pzm9eLtvuNjkQ,invalid",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
#[ignore = "requires RPC_URL and TEST_MARKET_PDA env vars"]
fn markets_info_live_single_market() {
    let rpc_url = std::env::var("RPC_URL").unwrap_or_default();
    let pda = std::env::var("TEST_MARKET_PDA").unwrap_or_default();
    if rpc_url.is_empty() || pda.is_empty() {
        eprintln!("Skip: set RPC_URL and TEST_MARKET_PDA to run");
        return;
    }
    let out = gmsol()
        .args(["markets-info", "--rpc-url", &rpc_url, "--market-pdas", &pda])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).expect("valid JSON");
    assert!(json.get("funding_factor_per_second").is_some());
    assert!(json.get("borrowing_factor_per_second_for_long").is_some());
    assert!(json.get("open_interest_long_usd").is_some());
}
