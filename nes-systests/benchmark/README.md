# Benchmark / Large Queries
The main goal of this directory is to provide a set of queries that can be used to test the performance of the system for large input data.
To this end, we have ported some of the queries from popular benchmarks like Nexmark, and Linear Road. 
We plan on extending this set of queries in the future, e.g., by adding queries from other benchmarks like Yahoo Streaming Benchmark, etc.
For now, we support the following benchmark queries:
- [Nexmark Benchmark](https://github.com/nexmark/nexmark/tree/master): Q1, Q2, Q5, Q8_Variant
- [Linear Road Benchmark](https://www.cs.brandeis.edu/~linearroad/linear-road.pdf): Q1 and Q2
- [Cluster Monitoring](https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md): Q2 from the [Lightsaber paper](https://lsds.doc.ic.ac.uk/sites/default/files/lightsaber-sigmod20.pdf)
- [Manufacturing](): Q1 from the [Lightsaber paper](https://lsds.doc.ic.ac.uk/sites/default/files/lightsaber-sigmod20.pdf)
- [Yahoo Streaming Benchmark](https://github.com/yahoo/streaming-benchmarks/tree/master)


Additionally, we have ported some of the queries from [DEBS Tutorial 2024](https://nebula.stream/publications/nebulastreamtutorial.html).
