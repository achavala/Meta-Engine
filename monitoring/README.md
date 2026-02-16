# Monitoring & Validation Tools

This directory contains tools to monitor and validate the performance of the new code fixes.

## Tools

### 1. `validation_monitor.py`

Comprehensive validation monitor that tracks:
- Trade execution rate (retry logic effectiveness)
- Selection gate filtering
- ORM score improvements
- Win rate and average returns

**Usage:**
```bash
# Analyze last 7 days
python3 monitoring/validation_monitor.py

# Analyze last 14 days
python3 monitoring/validation_monitor.py --days 14
```

**Output:**
- Console report with key metrics
- JSON report saved to `monitoring/reports/validation_report_YYYYMMDD_HHMMSS.json`

### 2. `compare_performance.py`

Compares current performance vs. baseline metrics and generates recommendations.

**Usage:**
```bash
# Compare last 7 days vs. baseline
python3 monitoring/compare_performance.py

# Save report to file
python3 monitoring/compare_performance.py --save

# Compare last 14 days
python3 monitoring/compare_performance.py --days 14
```

**Output:**
- Formatted comparison report
- Recommendations based on performance gaps
- Optional JSON report saved to `monitoring/reports/comparison_report_YYYYMMDD_HHMMSS.json`

## Baseline Metrics

Baseline metrics are from the Feb 9-12, 2026 analysis (before new code):

- **Trade Execution Rate:** 36.7%
- **Win Rate:** 8.3%
- **Average Winner Return:** +189.6%
- **Average Loser Return:** -12.3%

## Automatic Monitoring

The validation monitor runs automatically after each Meta Engine scan (non-blocking). Check logs for validation messages.

## Reports Directory

All reports are saved to `monitoring/reports/` with timestamps for historical tracking.

## Key Metrics Tracked

1. **Trade Execution Rate**
   - Target: 60-70% (vs. baseline 36.7%)
   - Monitors retry logic effectiveness

2. **Selection Gate Filtering**
   - Target: 5-10 candidates filtered per scan
   - Tracks ORM, signal count, and base score gates

3. **ORM Scores**
   - Target: 70-80% of winners have ORM ≥ 0.70
   - Monitors ORM weight enhancements

4. **Win Rate**
   - Target: 12-15% (vs. baseline 8.3%)
   - Tracks selection gate effectiveness

5. **Average Returns**
   - Target: Maintain +150-200% for winners
   - Monitors ORM enhancement impact

## Recommendations

The comparison tool automatically generates recommendations based on:
- Execution rate improvements
- Win rate improvements
- Selection gate effectiveness
- ORM score distributions
- Average return changes

## Integration

The validation monitor is integrated into `meta_engine.py` and runs automatically after each scan. It's non-blocking and logs errors only, so it won't interrupt the main pipeline.
