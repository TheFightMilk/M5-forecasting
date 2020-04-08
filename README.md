# M5-forecasting challenge
Repo contains the `R` files for the different models I have playing with on the M5 dataset.

There are some cool functions in the `xgb_agg_feat.R` file.
- `forcomb`: Simple median forecast combination of simple regression based models. Based on Hyndman's `forecast` package.
- `aggforecast`: Aggregates the product level sales to different aggregation levels (e.g. store, department, state, etc.) over time and forecasts over the test period using forcomb. The function is parallelized based on the number of serieses that needs to be forecasted. This can be controlled using the `par.threshold` argument. Say when aggregated by state, there are only 3 series and parallelizing might require more time. But aggregated at the item level there are 3049 time series that needs to be forecasted, and parallelizing saves a lot of time.
