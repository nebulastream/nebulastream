CREATE LOGICAL SOURCE bid(timestamp UINT64, auctionId INT32, bidder INT32, datetime INT64, price FLOAT64);
CREATE PHYSICAL SOURCE FOR bid TYPE File SET('TESTDATA/large/nexmark/bid_653M.csv' as `SOURCE`.FILE_PATH, 'CSV' as PARSER.`TYPE`);
-- test comment
CREATE LOGICAL SOURCE auction(timestamp UINT64,id INT32, initialbid INT32, reserve INT32, expires INT64, seller INT32, category INT32);
CREATE PHYSICAL SOURCE FOR auction TYPE File SET('TESTDATA/large/nexmark/auction_modified_74M.csv' as `SOURCE`.FILE_PATH, 'CSV' as PARSER.`TYPE`);

CREATE SINK q8Sink(bidauction.`start` UINT64, bidauction.`end` UINT64, bid_timestamp UINT64, bid.auctionId INT32, bid.bidder INT32, bid.datetime INT64, bid.price FLOAT64, auction_timestamp UINT64, auction.initialbid INT32, auction.reserve INT32, auction.expires INT64, auction.seller INT32, auction.category INT32)
 ---within sink
       TYPE File SET('WORKDIR/out.csv' as `SINK`.FILE_PATH, 'CSV' as `SINK`.INPUT_FORMAT);

SHOW QUERIES;
SELECT start, end, BID_TIMESTAMP, AUCTIONID, BIDDER, DATETIME, PRICE, AUCTION_TIMESTAMP, INITIALBID, RESERVE, EXPIRES, SELLER, CATEGORY
    FROM (SELECT *, TIMESTAMP as BID_TIMESTAMP FROM BID)
INNER JOIN
    (SELECT *, TIMESTAMP as AUCTION_TIMESTAMP FROM AUCTION)
ON AUCTIONID = ID WINDOW TUMBLING (TIMESTAMP, size 10 sec) INTO Q8SINK;

SHOW QUERIES;
SHOW QUERIES WHERE id = 1;
DROP QUERY 1;

SHOW QUERIES;
