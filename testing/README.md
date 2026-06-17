# Extra Configuration options

--worker.query_engine.number_of_worker_threads=6
--worker.query_engine.number_of_io_threads=2
--worker.query_engine.pin_threads=true
--worker.query_engine.dump_compilation_result=File

Setting 'pin_threads' to true will pin the IO threads on the first 'number_of_io_threads' cpus and the worker threads on the next (starting
with offset 'number_of_io_threads' ) 'number_of_worker_threads' cpus. 
