
def believer_score_query():
     query = """
MATCH (token:Token)
WITH token, token.marketCap AS marketCap
// Calculate believer score without market cap factor first
MATCH (wallet:Wallet)-[r:HOLDS]->(token)
WITH token, marketCap, count(wallet) AS num_wallets, collect(wallet) AS wallets
// Count wallets connected to Warpcast
MATCH (w:Wallet)-[r:HOLDS]->(token)
MATCH (w)-[:ACCOUNT*1..5]-(wc:Warpcast:Account)
WITH token, marketCap, num_wallets, count(DISTINCT w) AS warpcast_wallets
// Calculate percentage of wallets connected to Warpcast
WITH token, marketCap, num_wallets, warpcast_wallets,
     CASE 
          WHEN num_wallets = 0 THEN 0.0
          ELSE (toFloat(warpcast_wallets) * 100.0 / num_wallets)
     END AS warpcast_percentage

// Recalculate full believer score
MATCH (wallet:Wallet)-[r:HOLDS]->(token)
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wallet, r.balance AS balance
OPTIONAL MATCH path = (wallet)-[:ACCOUNT*1..5]-(wc:Warpcast)
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wc, wallet, balance
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage,
     // Calculate total balance for this token
     sum(balance) AS total_balance,
     // For Gini calculation
     collect({wallet: wallet, balance: balance}) AS wallet_data,
     // Calculate social data
     avg(wc.fcCredScore) AS avgSocialCredScore

// Calculate concentration score
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance, wallet_data, avgSocialCredScore
UNWIND wallet_data AS wallet_item
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance, wallet_item.balance/total_balance AS balance_fraction, avgSocialCredScore
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance, 
     // Calculate concentration using HHI
     sum(balance_fraction * balance_fraction) AS concentration_index,
     avgSocialCredScore
     
// Calculate believer score with concentration penalty
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, total_balance,
     concentration_index,
     // Concentration penalty - convert HHI to diversity score (1 - HHI)
     (1.0 - concentration_index) AS concentration_multiplier,
     avgSocialCredScore

// Calculate standard believer score
MATCH (wallet:Wallet)-[r:HOLDS]->(token)
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wallet, concentration_index, concentration_multiplier, avgSocialCredScore, total_balance
OPTIONAL MATCH path = (wallet)-[:ACCOUNT*1..5]-(wc:Warpcast)
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wc, collect(DISTINCT wallet) AS wallet_group, concentration_index, concentration_multiplier, avgSocialCredScore, total_balance
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, wc, wallet_group, concentration_index, concentration_multiplier, avgSocialCredScore, total_balance,
     CASE WHEN wc IS NULL THEN size(wallet_group)
          ELSE 1 + coalesce(wc.fcCredScore, 0)
     END AS group_weight

// Calculate raw and diversity-adjusted believer scores
WITH token, marketCap, num_wallets, warpcast_wallets, warpcast_percentage, 
     sum(group_weight) AS raw_score,
     concentration_index,
     concentration_multiplier, 
     avgSocialCredScore,
     total_balance

// Apply diversity adjustment
WITH token,
     raw_score AS raw_believer_score,
     raw_score * concentration_multiplier AS diversity_adjusted_score,
     marketCap,
     num_wallets,
     warpcast_wallets,
     warpcast_percentage,
     concentration_index,
     concentration_multiplier,
     avgSocialCredScore,
     total_balance

// Calculate holder-to-market cap ratio
WITH token, 
     raw_believer_score,
     diversity_adjusted_score,
     marketCap,
     num_wallets,
     warpcast_wallets,
     warpcast_percentage,
     concentration_index,
     concentration_multiplier,
     avgSocialCredScore,
     total_balance,
     // Direct holder-to-market cap ratio
     CASE 
          WHEN marketCap <= 0 THEN 999999999.0  // Prevent division by zero
          ELSE toFloat(num_wallets) / marketCap 
     END AS holder_mcap_ratio

// Apply LESS AGGRESSIVE tiered market cap penalty
WITH token, 
     raw_believer_score,
     diversity_adjusted_score,
     // Apply more forgiving tiered penalties
     CASE 
          WHEN holder_mcap_ratio > 2.0 THEN diversity_adjusted_score * 0.05  // Still severe but less extreme (95% reduction)
          WHEN holder_mcap_ratio > 1.0 THEN diversity_adjusted_score * 0.25  // Less severe (75% reduction)
          WHEN holder_mcap_ratio > 0.5 THEN diversity_adjusted_score * 0.50  // Moderate (50% reduction)
          WHEN holder_mcap_ratio > 0.2 THEN diversity_adjusted_score * 0.75  // Mild (25% reduction)
          ELSE diversity_adjusted_score  // No penalty
     END AS market_adjusted_score,
     marketCap,
     num_wallets,
     warpcast_wallets,
     warpcast_percentage,
     concentration_index,
     concentration_multiplier,
     avgSocialCredScore,
     total_balance,
     holder_mcap_ratio

// Get min/max values for normalization
WITH 
     MIN(market_adjusted_score) AS min_score, 
     MAX(market_adjusted_score) AS max_score,
     COLLECT({
          token: token, 
          raw_score: raw_believer_score,
          diversity_score: diversity_adjusted_score,
          market_score: market_adjusted_score,
          ratio: holder_mcap_ratio,
          marketCap: marketCap,
          num_wallets: num_wallets,
          warpcast_wallets: warpcast_wallets,
          warpcast_percentage: warpcast_percentage,
          concentration_index: concentration_index,
          concentration_multiplier: concentration_multiplier,
          avg_social: avgSocialCredScore,
          total_balance: total_balance
     }) AS all_token_data

// Normalize scores to 0-70
UNWIND all_token_data AS token_data
WITH token_data.token AS token, 
     token_data.raw_score AS raw_believer_score,
     token_data.diversity_score AS diversity_adjusted_score,
     token_data.market_score AS market_adjusted_score,
     token_data.ratio AS holder_mcap_ratio,
     token_data.marketCap AS marketCap,
     token_data.num_wallets AS num_wallets,
     token_data.warpcast_wallets AS warpcast_wallets,
     token_data.warpcast_percentage AS warpcast_percentage,
     token_data.concentration_index AS concentration_index,
     token_data.concentration_multiplier AS concentration_multiplier,
     token_data.avg_social AS avgSocialCredScore,
     token_data.total_balance AS total_balance,
     min_score, max_score

// Normalize to 0-70 scale and set properties on token nodes
WITH token, raw_believer_score, diversity_adjusted_score, market_adjusted_score, holder_mcap_ratio, 
     marketCap, num_wallets, warpcast_wallets, warpcast_percentage, 
     concentration_index, concentration_multiplier, avgSocialCredScore, total_balance,
     CASE 
          WHEN max_score = min_score THEN 40.0 // Default to middle value if all scores are equal
          ELSE 1.0 + 69.0 * (market_adjusted_score - min_score) / (max_score - min_score)
     END AS normalized_believer_score

// Set all properties on token nodes
SET token.believerScore = tofloat(normalized_believer_score),
    token.rawBelieverScore = tofloat(raw_believer_score), 
    token.diversityAdjustedScore = tofloat(diversity_adjusted_score),
    token.marketAdjustedScore = tofloat(market_adjusted_score),
    token.holderToMarketCapRatio = tofloat(holder_mcap_ratio),
    token.walletCount = tofloat(num_wallets),
    token.warpcastWallets = tofloat(warpcast_wallets),
    token.warpcastPercentage = tofloat(warpcast_percentage),
    token.concentrationIndex = tofloat(concentration_index),
    token.concentrationMultiplier = tofloat(concentration_multiplier),
    token.avgSocialCredScore = avgSocialCredScore,
    token.totalSupply = tofloat(total_balance),
    token.lastUpdateDt = datetime()
RETURN DISTINCT
     token.address, 
     token.believerScore,
     token.rawBeliverScore,
     token.diversityAdjustedScore,
     token.holderToMarketCapRatio,
     token.walletCount,
     token.warpcastWallets,
     token.warpcastPercentage,
     token.concentrationIndex,
     token.concentrationMultiplier,
     token.avgSocialCredScore,
     token.totalSupply,
     token.lastUpdateDt
    """
     return query 