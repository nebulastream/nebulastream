//
// Created by veronika on 20.05.24.
//

#include <StatisticCollection/StatisticStorage/BPlusTreeWithBucketing/Bucket.hpp>
#include <utility>

namespace NES::Statistic {

    Bucket::Bucket(const uint64_t startTs, const uint64_t endTs, const uint64_t bucketsRangeSize){
        this->endTs = endTs;
        this->startTs = startTs;
        this->bucketsRangeSize = bucketsRangeSize;

    }

    Bucket::Bucket(const uint64_t startTs, const uint64_t endTs, const uint64_t bucketsRangeSize, std::vector<StatisticPtr>& statisticsPtr)
    {
        this->endTs = endTs;
        this->startTs = startTs;
        this->bucketsRangeSize = bucketsRangeSize;
        this->statisticsPtr = statisticsPtr;
    }

    std::vector<StatisticPtr> Bucket::GetStatistics(){
        return statisticsPtr;
    }
    void Bucket::SetStatistics(std::vector<StatisticPtr>& NewStatistics){
        statisticsPtr = NewStatistics;
    }

    bool Bucket::deleteStatistic(const StatisticPtr& statistic) {
        // find the statistic if exists and delete it
        for (auto it = statisticsPtr.begin(); it != statisticsPtr.end(); ++it) {
            if (statistic->getStartTs().equals((*it)->getStartTs()) &&
                statistic->getEndTs().equals((*it)->getEndTs())) {
                statisticsPtr.erase(it);
                return true;
            }
        }
        return false;
    }

    bool Bucket::insertStatistic(const StatisticPtr& statistic) {
        // check for duplicate statistics
        for(const StatisticPtr& stat : statisticsPtr){
            if (statistic->getStartTs().equals(stat->getStartTs()) && statistic->getEndTs().equals(stat->getEndTs())) {
                return false;
            }
        }
        // insert if there is no duplicate
        statisticsPtr.push_back(statistic);
        return true;
    }

    uint64_t Bucket::getStartTs() const { return startTs; }
    uint64_t Bucket::getEndTs() const { return endTs; }

    }