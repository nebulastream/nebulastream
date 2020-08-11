#include <NodeEngine/QueryStatistics.hpp>
#include <sstream>
namespace NES {

const std::atomic<size_t> QueryStatistics::getProcessedTasks() const {
    return processedTasks.load();
}

const std::atomic<size_t> QueryStatistics::getProcessedTuple() const {
    return processedTuple.load();
}

const std::atomic<size_t> QueryStatistics::getProcessedBuffers() const {
    return processedBuffers.load();
}

const std::atomic<size_t> QueryStatistics::getProcessedWatermarks() const {
    return processedWatermarks.load();
}

void QueryStatistics::setProcessedTasks(const std::atomic<size_t>& processedTasks) {
    this->processedTasks = processedTasks.load();
}

void QueryStatistics::setProcessedTuple(const std::atomic<size_t>& processedTuple) {
    this->processedTuple = processedTuple.load();
}

void QueryStatistics::incProcessedBuffers() {
    this->processedBuffers++;
}

void QueryStatistics::incProcessedTasks() {
    this->processedTasks++;
}

void QueryStatistics::incProcessedWatermarks() {
    this->processedWatermarks++;
}
void QueryStatistics::incProcessedTuple(size_t tupleCnt) {
    this->processedTuple += tupleCnt;
}

void QueryStatistics::setProcessedBuffers(const std::atomic<size_t>& processedBuffers) {
    this->processedBuffers = processedBuffers.load();
}

std::string QueryStatistics::getQueryStatisticsAsString() {
    std::stringstream ss;
    ss << "processedTasks=" << processedTasks;
    ss << " processedTuple=" << processedTuple;
    ss << " processedBuffers=" << processedBuffers;
    ss << " processedWatermarks=" << processedWatermarks;
    return ss.str();
}
}

