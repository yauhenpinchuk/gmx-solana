use std::str::FromStr;

use anyhow::Context as _;
use gmsol_sdk::solana_utils::{solana_client::rpc_client::RpcClient, solana_sdk::pubkey::Pubkey};

use gmsol_markets_info_cli::{parse_market, CHUNK_SIZE};

/// Market info (funding, borrowing, OI) as JSON.
///
/// Replaces the former standalone `gmsol-markets-info-cli` binary.
/// Output format is identical: single PDA produces a JSON object,
/// multiple comma-separated PDAs produce a JSON array.
#[derive(Debug, clap::Args)]
pub struct MarketsInfo {
    /// Solana RPC URL.
    #[arg(long)]
    rpc_url: String,
    /// One market PDA or comma-separated list (one -> single object, many -> JSON array).
    #[arg(long)]
    market_pdas: String,
}

impl super::Command for MarketsInfo {
    fn is_client_required(&self) -> bool {
        false
    }

    async fn execute(&self, _ctx: super::Context<'_>) -> eyre::Result<()> {
        run(&self.rpc_url, &self.market_pdas).map_err(|e| eyre::eyre!(e))
    }
}

fn run(rpc_url: &str, market_pdas: &str) -> anyhow::Result<()> {
    let pdas: Vec<String> = market_pdas
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if pdas.is_empty() {
        anyhow::bail!("--market-pdas must not be empty (one PDA or comma-separated list)");
    }

    let client = RpcClient::new(rpc_url);

    if pdas.len() == 1 {
        let pda = &pdas[0];
        let pubkey = Pubkey::from_str(pda).context("invalid market_pda")?;
        let account = client
            .get_account(&pubkey)
            .with_context(|| format!("failed to fetch account {pubkey}"))?;
        let out = parse_market(account.data.as_slice(), None)?;
        println!("{}", serde_json::to_string(&out)?);
        return Ok(());
    }

    let pubkeys: Vec<Pubkey> = pdas
        .iter()
        .map(|s| Pubkey::from_str(s))
        .collect::<Result<Vec<_>, _>>()
        .context("invalid market_pda in list")?;
    let mut results = Vec::with_capacity(pubkeys.len());
    for chunk in pubkeys.chunks(CHUNK_SIZE) {
        let accounts = client
            .get_multiple_accounts(chunk)
            .context("get_multiple_accounts failed")?;
        for (i, opt) in accounts.into_iter().enumerate() {
            let pda = chunk.get(i).map(|p| p.to_string());
            match opt {
                Some(account) => match parse_market(account.data.as_slice(), pda.as_deref()) {
                    Ok(out) => results.push(out),
                    Err(e) => anyhow::bail!("market {}: {}", pda.as_deref().unwrap_or("?"), e),
                },
                None => anyhow::bail!("account not found: {}", pda.as_deref().unwrap_or("?")),
            }
        }
    }
    println!("{}", serde_json::to_string(&results)?);
    Ok(())
}
