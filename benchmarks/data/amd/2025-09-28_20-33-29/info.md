memory bounds and overall comparison (bounds 0 8 16 32 64 128 inf, min write sizes 0 4K 256K 1M 8M) final

batch_size: 100000
ingestion_rate: 100000000
memory_bounds: all
match_rate: 70
watermark_gaps: 1
min_write_size: 0 4K 256K 1M 8M
slice_store: FILE_BACKED
queries: 1-6
timestamp_increment: 1
watermark_predictor: RLS
with_prediction: all