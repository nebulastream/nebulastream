//
// Created by veronika on 20.05.24.
//

#ifndef NES_BUCKETS_HPP
#define NES_BUCKETS_HPP

#include <Statistics/Statistic.hpp>
#include <Statistics/StatisticKey.hpp>
#include <optional>
#include <Measures/TimeMeasure.hpp>

class Statistic;
using StatisticPtr = std::shared_ptr<Statistic>;


namespace NES::Statistic {
class Bucket{
  protected:
    unsigned long startTs;
    unsigned long endTs;
    unsigned long bucketsRangeSize;
    std::vector<StatisticPtr> statisticsPtr;

  public:

    //empty bucket
    Bucket(uint64_t startTs, uint64_t  endTs,uint64_t bucketsRangeSize);

    Bucket(uint64_t startTs,uint64_t  endTs, uint64_t  bucketsRangeSize,std::vector<StatisticPtr>& statisticsPtr);

    // Getter for statistics
    std::vector<StatisticPtr> GetStatistics();
    // Setter for statistics
    void SetStatistics(std::vector<StatisticPtr>& NewStatistics);

    bool deleteStatistic(const StatisticPtr& statisticPtr);
    bool insertStatistic(const StatisticPtr& statisticPtr);

    [[nodiscard]] uint64_t getStartTs() const;
    [[nodiscard]] uint64_t getEndTs() const;



};

}

#endif//NES_BUCKETS_HPP