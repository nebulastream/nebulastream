#include <Execution/StatisticsCollector/StatisticsCollector.hpp>
#include <Execution/StatisticsCollector/Statistic.hpp>
#include <list>

namespace NES::Runtime::Execution {

void StatisticsCollector::addStatistic(std::shared_ptr<Statistic> statistic) {
    listOfStatistics.push_back(statistic);
}

} // namespace NES::Runtime::Execution