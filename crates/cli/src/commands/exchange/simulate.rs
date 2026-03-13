//! Handler for `gmsol exchange simulate-increase`.
//!
//! Fetches on-chain market state, queries Pyth Hermes for current prices,
//! runs the GMX order simulation off-chain, and prints the result as JSON
//! to stdout. No transaction is submitted.

use std::{collections::HashMap, sync::Arc};

use anchor_spl::token_interface::Mint;
use eyre::{bail, eyre, OptionExt};
use gmsol_sdk::model::price::Price;
use gmsol_sdk::{
    builders::order::{CreateOrderKind, CreateOrderParams},
    client::token_map::TokenMap,
    core::{
        oracle::{pyth_price_with_confidence_to_price, PriceProviderKind},
        token_config::TokenMapAccess,
    },
    programs::{anchor_lang::prelude::Pubkey, gmsol_store::accounts::Market},
    simulation::{
        order::OrderSimulationOutput,
        simulator::{SimulationOptions, Simulator, TokenState},
    },
};
use serde::Serialize;

use crate::CommandClient;

// ---------------------------------------------------------------------------
// Hermes HTTP types
// ---------------------------------------------------------------------------

const HERMES_BASE: &str = "https://hermes.pyth.network";
const HERMES_LATEST: &str = "/v2/updates/price/latest";

/// Minimal deserialisation of a Hermes price-update response.
#[derive(serde::Deserialize)]
struct HermesResponse {
    #[serde(default)]
    parsed: Vec<HermesParsedUpdate>,
}

#[derive(serde::Deserialize)]
struct HermesParsedUpdate {
    id: String,
    price: HermesPrice,
}

/// Price entry from Hermes `parsed[]`.
/// The Hermes API returns `price` and `conf` as decimal strings.
#[derive(serde::Deserialize)]
struct HermesPrice {
    #[serde(with = "serde_i64_string")]
    price: i64,
    #[serde(with = "serde_u64_string")]
    conf: u64,
    expo: i32,
}

mod serde_i64_string {
    use serde::{de::Error, Deserialize, Deserializer};
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

mod serde_u64_string {
    use serde::{de::Error, Deserialize, Deserializer};
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        String::deserialize(d)?.parse().map_err(D::Error::custom)
    }
}

// ---------------------------------------------------------------------------
// JSON output contract
// ---------------------------------------------------------------------------

/// JSON printed to stdout on success.
#[derive(Serialize)]
struct SimulateOutput {
    index_price: String,
    execution_price: String,
    price_impact_usd: String,
    price_impact_pct: String,
    size_delta_tokens: String,
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

/// Run the simulate-increase handler.
///
/// `collateral_amount` is the raw token-unit amount (e.g. lamports for SOL).
/// `size_u128` is the position size in 30-decimal USD (= size_usd * 10^30).
pub(crate) async fn run(
    client: &CommandClient,
    _store: &Pubkey,
    market_token: &Pubkey,
    market: &Market,
    token_map: &TokenMap,
    is_long: bool,
    is_collateral_long: bool,
    collateral_amount: u64,
    size_u128: u128,
) -> eyre::Result<()> {
    // ------------------------------------------------------------------
    // 1. Fetch market-token mint supply (needed for MarketModel).
    // ------------------------------------------------------------------
    let mint: Mint = client
        .account::<Mint>(market_token)
        .await?
        .ok_or_eyre("market token mint account not found")?;
    let supply = mint.supply;

    // ------------------------------------------------------------------
    // 2. Look up token configs for the three tokens in this market.
    // ------------------------------------------------------------------
    let meta = &market.meta;
    let index_config = token_map
        .get(&meta.index_token_mint)
        .ok_or_eyre("missing token config for index token")?;
    let long_config = token_map
        .get(&meta.long_token_mint)
        .ok_or_eyre("missing token config for long token")?;
    let short_config = token_map
        .get(&meta.short_token_mint)
        .ok_or_eyre("missing token config for short token")?;

    let index_token_decimals = index_config.token_decimals;

    // ------------------------------------------------------------------
    // 3. Get Pyth feed IDs for each token.
    // ------------------------------------------------------------------
    let index_feed = index_config
        .get_feed(&PriceProviderKind::Pyth)
        .map_err(|e| eyre!("index token has no Pyth feed: {e}"))?;
    let long_feed = long_config
        .get_feed(&PriceProviderKind::Pyth)
        .map_err(|e| eyre!("long token has no Pyth feed: {e}"))?;
    let short_feed = short_config
        .get_feed(&PriceProviderKind::Pyth)
        .map_err(|e| eyre!("short token has no Pyth feed: {e}"))?;

    // Convert Pubkey bytes to hex feed-ID strings (no 0x prefix, matching
    // what Pyth Hermes expects in the `ids[]` query parameter).
    let index_hex = hex::encode(index_feed.to_bytes());
    let long_hex = hex::encode(long_feed.to_bytes());
    let short_hex = hex::encode(short_feed.to_bytes());

    // ------------------------------------------------------------------
    // 4. Query Hermes for the latest prices.
    // ------------------------------------------------------------------
    let http = reqwest::Client::new();
    let url = format!("{HERMES_BASE}{HERMES_LATEST}");
    let resp: HermesResponse = http
        .get(&url)
        .query(&[
            ("ids[]", index_hex.as_str()),
            ("ids[]", long_hex.as_str()),
            ("ids[]", short_hex.as_str()),
            ("parsed", "true"),
        ])
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    if resp.parsed.is_empty() {
        bail!("Hermes returned no parsed price updates");
    }

    // Build a map from (lower-case) feed id → HermesPrice.
    let price_map: HashMap<String, &HermesPrice> = resp
        .parsed
        .iter()
        .map(|u| (u.id.to_lowercase(), &u.price))
        .collect();

    let get_price = |feed_pubkey: &Pubkey, config: &gmsol_sdk::core::token_config::TokenConfig| {
        let hex = hex::encode(feed_pubkey.to_bytes());
        let hp = price_map
            .get(&hex)
            .ok_or_else(|| eyre!("Hermes missing price for feed {hex}"))?;
        let p = pyth_price_with_confidence_to_price(hp.price, hp.conf, hp.expo, config)
            .map_err(|e| eyre!("price conversion error: {e}"))?;
        Ok::<Price<u128>, eyre::Error>(Price {
            min: p.min.to_unit_price(),
            max: p.max.to_unit_price(),
        })
    };

    let index_price = get_price(&index_feed, index_config)?;
    let long_price = get_price(&long_feed, long_config)?;
    let short_price = get_price(&short_feed, short_config)?;

    // ------------------------------------------------------------------
    // 5. Build Simulator and run the order simulation.
    // ------------------------------------------------------------------
    let market_model =
        gmsol_sdk::programs::model::MarketModel::from_parts(Arc::new(market.clone()), supply);

    let mut tokens: HashMap<Pubkey, TokenState> = HashMap::new();
    tokens.insert(
        meta.index_token_mint,
        TokenState::from_price(Some(Arc::new(index_price))),
    );
    tokens.insert(
        meta.long_token_mint,
        TokenState::from_price(Some(Arc::new(long_price))),
    );
    tokens.insert(
        meta.short_token_mint,
        TokenState::from_price(Some(Arc::new(short_price))),
    );

    let mut markets: HashMap<Pubkey, gmsol_sdk::programs::model::MarketModel> = HashMap::new();
    markets.insert(*market_token, market_model);

    let mut simulator = Simulator::from_parts(
        tokens,
        markets,
        HashMap::new(), // no GLV needed
        Default::default(), // no VI needed
    );

    let collateral_token = if is_collateral_long {
        meta.long_token_mint
    } else {
        meta.short_token_mint
    };

    let params = CreateOrderParams::builder()
        .market_token(*market_token)
        .is_long(is_long)
        .amount(collateral_amount as u128)
        .size(size_u128)
        .build();

    let output = simulator
        .simulate_order(
            CreateOrderKind::MarketIncrease,
            &params,
            &collateral_token,
        )
        .build()
        .execute_with_options(SimulationOptions {
            disable_vis: true,
            skip_limit_price_validation: true,
        })?;

    let (execution_price_u128, price_impact_value_i128, size_delta_tokens_u128) = match output {
        OrderSimulationOutput::Increase { report, .. } => {
            let exec = report.execution();
            (
                *exec.execution_price(),
                *exec.price_impact_value(),
                *exec.size_delta_in_tokens(),
            )
        }
        _ => bail!("unexpected simulation output kind"),
    };

    // ------------------------------------------------------------------
    // 6. Scale and output JSON.
    // ------------------------------------------------------------------
    // Contract for Python (`order_simulator.py`): every *price* field is an integer
    // such that `field / 10^30` = human USD (per 1 full index token).
    //
    // Index: oracle unit price is per smallest index unit; scale up to per 1 full token.
    let dec_factor = 10u128.pow(index_token_decimals as u32);
    let index_price_json = (index_price.min as u128)
        .checked_mul(dec_factor)
        .ok_or_else(|| eyre!("index_price overflow when scaling"))?;

    // Execution USD per full index token must use (size_usd * 10^dec) / tokens, not
    // (size_usd / tokens) * dec: the model stores execution_price = size_delta_usd /
    // size_delta_in_tokens in one step; u128 division there can truncate to a tiny value
    // when tokens are large, which broke downstream acceptable-price in the dashboard.
    let execution_price_json = if size_delta_tokens_u128 > 0 {
        size_u128
            .checked_mul(dec_factor)
            .and_then(|n| n.checked_div(size_delta_tokens_u128))
            .ok_or_else(|| eyre!("execution_price_json overflow or divide by zero"))?
    } else {
        index_price_json
    };

    // price_impact_value is already in 30-decimal USD; output as-is.
    let price_impact_usd_json = price_impact_value_i128;

    // price_impact_pct = impact_usd_30dec / size_usd_30dec  (ratio, both in same scale → 10^30 cancels)
    let price_impact_pct: f64 = if size_u128 == 0 {
        0.0
    } else {
        price_impact_value_i128 as f64 / size_u128 as f64
    };

    let out = SimulateOutput {
        index_price: index_price_json.to_string(),
        execution_price: execution_price_json.to_string(),
        price_impact_usd: price_impact_usd_json.to_string(),
        price_impact_pct: format!("{price_impact_pct:.6}"),
        size_delta_tokens: size_delta_tokens_u128.to_string(),
    };

    println!("{}", serde_json::to_string(&out)?);
    Ok(())
}
