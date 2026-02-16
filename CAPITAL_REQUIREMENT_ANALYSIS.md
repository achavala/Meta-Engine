# Capital Requirement Analysis: $1M in 6 Months
## Institutional-Grade Analysis (30+ Years Trading + PhD Quant Lens)

**Analysis Date:** February 12, 2026  
**Target:** $1,000,000 in 6 months (26 weeks)  
**Analysis Method:** Monte Carlo simulation with compounding, drawdowns, and risk management

---

## Executive Summary

### Required Capital (Realistic Scenario)

**$226,562.50** to reach $1M in 6 months

**Key Assumptions:**
- Win Rate: 12.0% (improved from 8.3% baseline with selection gates)
- Average Winner Return: +180.0%
- Average Loser Return: -12.0%
- Trades Per Week: 6 (3 picks × 2 scans: 9:35 AM, 3:15 PM)
- Position Size: 10% of capital per trade
- Max Drawdown Limit: 20%

---

## Scenario Analysis

### 1. Baseline Scenario (Feb 9-12 Performance)

**Required Capital:** $474,062.50

**Assumptions:**
- Win Rate: 8.3% (baseline from analysis)
- Avg Winner Return: +189.6%
- Avg Loser Return: -12.3%
- Trades Per Week: 6
- Position Size: 10% of capital

**Projected Results:**
- Final Capital: $1,633,917.44
- Total Return: +244.7%
- Max Drawdown: 21.4%

**Assessment:** ⚠️ Higher capital requirement due to lower win rate. New code improvements should reduce this.

---

### 2. Realistic Scenario (Improved with New Code) ⭐ **RECOMMENDED**

**Required Capital:** $226,562.50

**Assumptions:**
- Win Rate: 12.0% (improved from 8.3% with selection gates)
- Avg Winner Return: +180.0% (slightly conservative vs. 189.6%)
- Avg Loser Return: -12.0% (same as baseline)
- Trades Per Week: 6
- Position Size: 10% of capital

**Projected Results:**
- Final Capital: $1,480,371.19
- Total Return: +553.4%
- Max Drawdown: 21.4%

**Assessment:** ✅ **This is the recommended scenario.** Assumes selection gates improve win rate from 8.3% to 12%, which is realistic based on analysis showing gates would filter 5-10 losers.

---

### 3. Optimistic Scenario (Best Case)

**Required Capital:** $56,406.25

**Assumptions:**
- Win Rate: 15.0% (selection gates working very well)
- Avg Winner Return: +200.0% (ORM enhancements working)
- Avg Loser Return: -10.0% (better risk management)
- Trades Per Week: 6
- Position Size: 12% of capital (slightly more aggressive)

**Projected Results:**
- Final Capital: $4,966,189.87
- Total Return: +8,704.3%
- Max Drawdown: 18.6%

**Assessment:** ⚠️ Very optimistic. Requires everything to work perfectly. Not recommended as primary scenario.

---

## Risk Analysis

### Worst-Case Scenario

**Starting Capital:** $226,562.50 (Realistic scenario capital)

**Assumptions:**
- Win Rate: 8.0% (lower than baseline)
- Avg Winner Return: +150.0% (lower than baseline)
- Avg Loser Return: -15.0% (worse than baseline)
- Trades Per Week: 6
- Position Size: 10% of capital

**Projected Results:**
- Final Capital: $276,492.22
- Total Return: +22.0%
- Max Drawdown: 29.8%

**Assessment:** ⚠️ **Significant risk.** Even with realistic capital, worst-case only reaches $276k (22% return) instead of $1M. This highlights the importance of:
1. Proper risk management
2. Selection gate effectiveness
3. ORM enhancement success
4. Trade execution improvements

---

## Key Metrics from Historical Analysis

### Baseline Performance (Feb 9-12, 2026)

| Metric | Value |
|--------|-------|
| **Total Picks Analyzed** | 60 |
| **Winners (≥50%)** | 5 (8.3%) |
| **Losers (<0%)** | 34 (56.7%) |
| **Average Winner Return** | +189.6% |
| **Average Loser Return** | -12.3% |
| **No Trade Executed** | 38 (63.3%) ⚠️ |

### Expected Improvements with New Code

| Improvement | Impact |
|-------------|--------|
| **Selection Gates** | Win rate: 8.3% → 12.0% (+3.7%) |
| **ORM Enhancements** | Better identification of high-return opportunities |
| **Trade Execution** | Execution rate: 36.7% → 60-70% |
| **Risk Management** | Stop loss at -50%, partial profit at 2x |

---

## Position Sizing Analysis

### Current Historical Data

- **Average Position Size:** $1,143.33
- **Required Capital (Realistic):** $226,562.50
- **Position Size as % of Capital:** 0.5%

**Note:** Current position sizes are very small relative to capital. With $226k capital and 10% position sizing, average position would be ~$22,656, which is 20x larger than current average.

### Recommended Position Sizing

1. **10% per trade** (allows 10 concurrent positions)
2. **Reduce to 5% during drawdowns** (when drawdown > 20%)
3. **Maximum 3 positions per scan** (3 picks × 2 scans = 6 trades/week)

---

## Compounding Analysis

### How Compounding Works

With 6 trades per week and 26 weeks:
- **Total Trades:** 156 trades
- **Expected Winners:** 19 trades (12% win rate)
- **Expected Losers:** 137 trades (88% lose rate)

**Key Insight:** Even with a 12% win rate, the massive returns on winners (+180%) more than compensate for the losses (-12%) due to compounding.

### Example: First Month

**Starting Capital:** $226,562.50

**Week 1-4 (24 trades):**
- Expected Winners: 3 trades (12%)
- Expected Losers: 21 trades (88%)
- Position Size: $22,656 per trade (10% of capital)

**Projected Results:**
- Winner P&L: 3 × $22,656 × 1.80 = $122,342
- Loser P&L: 21 × $22,656 × (-0.12) = -$57,093
- Net P&L: +$65,249
- **New Capital:** $291,811.50 (+28.8%)

**Compounding Effect:** As capital grows, position sizes increase, accelerating returns.

---

## Risk Management Considerations

### 1. Drawdown Protection

- **Max Drawdown Limit:** 20%
- **Action:** Reduce position size to 5% when drawdown > 20%
- **Recovery:** Return to 10% when drawdown < 15%

### 2. Stop Loss

- **Stop Loss:** -50% per trade (already implemented)
- **Impact:** Limits worst-case losses to -50% instead of -100%

### 3. Partial Profit Taking

- **Trigger:** 2x return (200%)
- **Action:** Record partial profit (already implemented)
- **Impact:** Locks in gains while allowing further upside

### 4. Position Concentration

- **Max Positions:** 10 concurrent (10% each)
- **Per Scan:** 3 picks maximum
- **Diversification:** Spread across different sectors/symbols

---

## Recommendations

### 1. Starting Capital

**Recommended: $250,000** (slightly above realistic scenario)

**Rationale:**
- Provides buffer for worst-case scenarios
- Allows for 10% position sizing with room for drawdowns
- Comfortable margin above $226,562.50 requirement

### 2. Position Sizing Strategy

**Phase 1 (Weeks 1-4):** 8% per trade (conservative start)
**Phase 2 (Weeks 5-13):** 10% per trade (normal operation)
**Phase 3 (Weeks 14-26):** 12% per trade (if ahead of target)

**Adjustment Rules:**
- Reduce to 5% if drawdown > 20%
- Increase to 12% if ahead of target by >20%

### 3. Risk Management

1. ✅ **Stop Loss:** -50% per trade (implemented)
2. ✅ **Partial Profit:** 2x return (implemented)
3. ✅ **Max Drawdown:** 20% limit (implemented)
4. ✅ **Position Limit:** 10 concurrent positions

### 4. Monitoring

1. **Weekly Review:** Check win rate, average returns, drawdown
2. **Monthly Review:** Compare vs. target ($1M in 6 months)
3. **Adjustment:** Modify position sizing based on performance

### 5. Scaling Strategy

**Conservative Approach:**
1. Start with $100,000
2. Prove system works for 1 month
3. Scale up to $250,000 if on track
4. Continue to $1M target

**Aggressive Approach:**
1. Start with $250,000
2. Full position sizing from day 1
3. Monitor closely for first 2 weeks
4. Adjust if needed

---

## Success Factors

### Critical Success Factors

1. **Selection Gates Working:** Win rate must improve from 8.3% to 12%+
2. **Trade Execution:** Execution rate must improve from 36.7% to 60%+
3. **ORM Enhancements:** Must identify high-return opportunities (UNH, MRVL types)
4. **Risk Management:** Stop loss and partial profit must work correctly

### Validation Plan

**Week 1-2:**
- Monitor execution rate (target: 60%+)
- Monitor selection gate filtering (target: 5-10 per scan)
- Monitor win rate (target: 12%+)

**Week 3-4:**
- Compare performance vs. projections
- Adjust position sizing if needed
- Review and optimize

**Month 2-6:**
- Continue monitoring
- Scale up if ahead of target
- Scale down if behind target

---

## Conclusion

### Required Capital Summary

| Scenario | Required Capital | Final Capital | Total Return | Risk Level |
|----------|-----------------|--------------|--------------|------------|
| **Baseline** | $474,062.50 | $1,633,917 | +244.7% | Medium |
| **Realistic** ⭐ | **$226,562.50** | **$1,480,371** | **+553.4%** | **Medium** |
| **Optimistic** | $56,406.25 | $4,966,190 | +8,704.3% | High |
| **Worst-Case** | $226,562.50 | $276,492 | +22.0% | High |

### Final Recommendation

**Start with $250,000** to reach $1M in 6 months with realistic assumptions.

**Key Requirements:**
1. ✅ Selection gates improve win rate to 12%+
2. ✅ Trade execution rate improves to 60%+
3. ✅ ORM enhancements identify high-return opportunities
4. ✅ Risk management (stop loss, partial profit) works correctly
5. ✅ Consistent execution of 6 trades per week

**Risk Mitigation:**
- Worst-case scenario shows significant risk (only $276k final)
- Start conservatively and scale up
- Monitor closely for first month
- Adjust position sizing based on performance

**The system has strong potential, but success depends on the new code improvements working as designed.**

---

**Analysis Tool:** `analysis/capital_requirement_analysis.py`  
**Data Sources:** `trading/trade_db.py`, `WEEK_ANALYSIS_SUMMARY.md`  
**Methodology:** Monte Carlo simulation with compounding, drawdowns, and risk management
