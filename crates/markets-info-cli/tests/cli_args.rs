//! Integration tests: run gmsol-markets-info-cli binary and check exit codes and output.

use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_gmsol-markets-info-cli"))
}

#[test]
fn help_exits_zero() {
    let out = bin().arg("--help").output().unwrap();
    assert!(out.status.success(), "stderr: {}", String::from_utf8_lossy(&out.stderr));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("--rpc-url"));
    assert!(stdout.contains("--market-pdas"));
}

#[test]
fn no_args_fails() {
    let out = bin().output().unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("required") || stderr.contains("Usage"));
}

#[test]
fn rpc_url_only_no_market_fails() {
    let out = bin()
        .args(["--rpc-url", "https://api.mainnet-beta.solana.com"])
        .output()
        .unwrap();
    assert!(!out.status.success());
}

#[test]
fn empty_market_pdas_fails() {
    let out = bin()
        .args([
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
fn market_pdas_only_commas_fails() {
    let out = bin()
        .args([
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
fn invalid_pda_fails() {
    let out = bin()
        .args([
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
fn invalid_pda_in_batch_fails() {
    let out = bin()
        .args([
            "--rpc-url",
            "https://api.mainnet-beta.solana.com",
            "--market-pdas",
            "CJg17Dn4xgUyEW3gKSSyteNw7LhP1o9pzm9eLtvuNjkQ,invalid",
        ])
        .output()
        .unwrap();
    assert!(!out.status.success());
}
