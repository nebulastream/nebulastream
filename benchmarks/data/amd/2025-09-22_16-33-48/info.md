memory bounds and overall comparison (bounds 0 8 16 32 64 128 inf, min write sizes 0 4K 8M) final

batch_size: 100000
ingestion_rate: 100000000
memory_bounds: all
match_rate: 70
watermark_gaps: 10
min_write_size: 0 4K 8M
slice_store: all
queries: 1-9
timestamp_increment: 1
watermark_predictor: KALMAN
with_prediction: all