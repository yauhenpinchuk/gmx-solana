//! Library for parsing GMX-Solana Market account data into status (funding, borrowing, OI).

use std::mem;

use anyhow::Context;
use bytemuck::pod_read_unaligned;
use serde::Serialize;

use gmsol_model::pool::Balance;
use gmsol_model::price::{Price, Prices};
use gmsol_model::BaseMarketExt;
use gmsol_store::states::market::status::MarketStatus;
use gmsol_store::states::Market;

/// Max number of PDAs per getMultipleAccounts request (RPC limit).
pub const CHUNK_SIZE: usize = 100;

#[derive(Clone, Debug, Serialize)]
pub struct Output {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_pda: Option<String>,
    pub funding_factor_per_second: i128,
    pub borrowing_factor_per_second_for_long: u128,
    pub borrowing_factor_per_second_for_short: u128,
    pub open_interest_long_usd: u128,
    pub open_interest_short_usd: u128,
}

/// Parse Market account data (8-byte discriminator + Market struct) into status.
pub fn parse_market(data: &[u8], market_pda: Option<&str>) -> anyhow::Result<Output> {
    let min_len = 8 + mem::size_of::<Market>();
    anyhow::ensure!(
        data.len() >= min_len,
        "account data too small for Market (need at least {} bytes, got {})",
        min_len,
        data.len()
    );
    let market: Market = pod_read_unaligned(&data[8..8 + mem::size_of::<Market>()]);

    let p = 100_000_000_000_u128;
    let prices = Prices {
        index_token_price: Price { min: p, max: p },
        long_token_price: Price { min: p, max: p },
        short_token_price: Price { min: p, max: p },
    };

    let status =
        MarketStatus::from_market(&market, &prices, false, false).context("from_market")?;
    let oi = market
        .open_interest()
        .map_err(gmsol_model::Error::from)
        .context("open_interest")?;
    let open_interest_long_usd = oi
        .long_amount()
        .map_err(gmsol_model::Error::from)
        .context("oi long")?;
    let open_interest_short_usd = oi
        .short_amount()
        .map_err(gmsol_model::Error::from)
        .context("oi short")?;

    Ok(Output {
        market_pda: market_pda.map(String::from),
        funding_factor_per_second: status.funding_factor_per_second,
        borrowing_factor_per_second_for_long: status.borrowing_factor_per_second_for_long,
        borrowing_factor_per_second_for_short: status.borrowing_factor_per_second_for_short,
        open_interest_long_usd,
        open_interest_short_usd,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_market_rejects_empty_data() {
        let err = parse_market(&[], None).unwrap_err();
        assert!(err.to_string().contains("too small"), "{}", err);
    }

    #[test]
    fn parse_market_rejects_data_smaller_than_market() {
        let buf = vec![0u8; 8 + 100]; // way smaller than size_of::<Market>()
        let err = parse_market(&buf, None).unwrap_err();
        // Should fail either on size check or on from_market/open_interest
        let s = err.to_string();
        assert!(
            s.contains("too small") || s.contains("from_market") || s.contains("open_interest"),
            "{}",
            s
        );
    }

    #[test]
    fn chunk_size_is_within_rpc_limit() {
        assert!(CHUNK_SIZE <= 100, "getMultipleAccounts typically allows up to 100 keys");
    }
}
