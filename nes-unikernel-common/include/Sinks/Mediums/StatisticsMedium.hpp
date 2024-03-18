
#ifndef STATISTICSMEDIUM_HPP
#define STATISTICSMEDIUM_HPP

struct Counter {
    explicit Counter(const std::chrono::seconds& duration) : duration(duration) {}
    std::chrono::seconds duration;
    std::chrono::high_resolution_clock::time_point last_interval = std::chrono::high_resolution_clock::now();
    std::atomic<uint64_t> counter = 0;
    size_t last_counter = 0;
    std::atomic_bool running = true;
    std::jthread runner = std::jthread([this]() {
        while (running.load()) {
            std::this_thread::sleep_for(duration);
            auto now = std::chrono::high_resolution_clock::now();
            auto current_counter = counter.load();
            if (current_counter > last_counter) {
                const auto elapsedTime = std::chrono::duration_cast<std::chrono::duration<double>>(now - last_interval);
                NES_INFO("Received {} tuples in {}s ({}tps)",
                         (current_counter - last_counter),
                         elapsedTime.count(),
                         static_cast<double>((current_counter - last_counter)) / elapsedTime.count());
            }
            last_interval = now;
            last_counter = current_counter;
        }
    });
};

namespace NES {
class StatisticsMedium : public SinkMedium {
  public:
    StatisticsMedium(SinkFormatPtr sinkFormat,
                     uint32_t numOfProducers,
                     QueryId queryId,
                     QuerySubPlanId querySubPlanId,
                     std::chrono::seconds duration)
        : SinkMedium(std::move(sinkFormat), numOfProducers, queryId, querySubPlanId), counter(std::move(duration)) {}
    void setup() override;
    void shutdown() override {}
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) override;
    std::string toString() const override;
    SinkMediumTypes getSinkMediumType() override;

    Counter counter;
};

inline void StatisticsMedium::setup() {}

inline bool StatisticsMedium::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    auto numberOfTuples = inputBuffer.getNumberOfTuples();
    counter.counter.fetch_add(numberOfTuples);
    return true;
}

inline std::string StatisticsMedium::toString() const { return "StatisticsMedium"; }
inline SinkMediumTypes StatisticsMedium::getSinkMediumType() { return SinkMediumTypes::MONITORING_SINK; }
}// namespace NES
#endif//STATISTICSMEDIUM_HPP
