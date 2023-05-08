#ifndef NES_INCLUDE_DATAGENERATORS_NEXTMARK_NEXMARKCOMMON_HPP_
#define NES_INCLUDE_DATAGENERATORS_NEXTMARK_NEXMARKCOMMON_HPP_
namespace NES::Benchmark::DataGeneration {

class NexmarkCommon {
  public:
    static const long PERSON_EVENT_RATIO = 1;
    static const long AUCTION_EVENT_RATIO = 4;
    static const long BID_EVENT_RATIO = 4;
    static const long TOTAL_EVENT_RATIO = PERSON_EVENT_RATIO + AUCTION_EVENT_RATIO + BID_EVENT_RATIO;

    static const int MAX_PARALLELISM = 50;

    static const long START_ID_AUCTION[MAX_PARALLELISM];
    static const long START_ID_PERSON[MAX_PARALLELISM];

    static const long MAX_PERSON_ID = 540000000L;
    static const long MAX_AUCTION_ID = 540000000000L;
    static const long MAX_BID_ID = 540000000000L;

    static const int HOT_SELLER_RATIO = 100;
    static const int HOT_AUCTIONS_PROB = 85;
    static const int HOT_AUCTION_RATIO = 100;
};

}// namespace
#endif//NES_INCLUDE_DATAGENERATORS_NEXTMARK_NEXMARKCOMMON_HPP_
