CREATE LOGICAL SOURCE testSource (attr1 UINT32);

CREATE PHYSICAL SOUREC FOR testSource TYPE File SET ('/tmp/nebulastream/cmake-build-debug-docker/nes-systests/testdata/small/stream.csv' AS `SOURCE`.FILE_PATH, 'CSV' PARSER.`TYPE`);


