
#ifndef STATISTICSMEDIUM_HPP
#define STATISTICSMEDIUM_HPP
namespace NES {
class StatisticsMedium : public SinkMedium {
  public:
    StatisticsMedium(SinkFormatPtr sinkFormat,
                     uint32_t numOfProducers,
                     QueryId queryId,
                     QuerySubPlanId querySubPlanId,
                     std::chrono::seconds duration)
        : SinkMedium(std::move(sinkFormat), numOfProducers, queryId, querySubPlanId), duration(std::move(duration)) {}
    void setup() override;
    void shutdown() override;
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) override;
    std::string toString() const override;
    SinkMediumTypes getSinkMediumType() override;

    std::chrono::seconds duration;
    std::chrono::high_resolution_clock::time_point last_interval = std::chrono::high_resolution_clock::now();
    size_t totalCounter = 0;
    size_t newTupleCounter = 0;
};
inline void StatisticsMedium::setup() {}
inline void StatisticsMedium::shutdown() {}
inline bool StatisticsMedium::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    newTupleCounter += inputBuffer.getNumberOfTuples();
    const auto current = std::chrono::high_resolution_clock::now();
    if (last_interval + duration < current) {
        const auto newCounter = totalCounter + newTupleCounter;
        const auto elapsedTime = std::chrono::duration_cast<std::chrono::duration<double>>(current - last_interval);

        NES_INFO("Received {} tuples in {}s ({}tps)",
                 (newCounter - totalCounter),
                 elapsedTime.count(),
                 static_cast<double>((newCounter - totalCounter)) / elapsedTime.count());

        last_interval = current;
        newTupleCounter = 0;
        totalCounter = newCounter;
    }

    return true;
}
inline std::string StatisticsMedium::toString() const { return "StatisticsMedium"; }
inline SinkMediumTypes StatisticsMedium::getSinkMediumType() { return SinkMediumTypes::MONITORING_SINK; }
}// namespace NES
#endif//STATISTICSMEDIUM_HPP
