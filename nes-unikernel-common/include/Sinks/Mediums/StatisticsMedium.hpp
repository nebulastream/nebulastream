
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
class LatencySink : public SinkMedium {
    std::chrono::time_point<std::chrono::system_clock> start = std::chrono::system_clock::now();
    std::chrono::time_point<std::chrono::high_resolution_clock> start_hr = std::chrono::high_resolution_clock::now();
    std::chrono::milliseconds allowedDelay;

  public:
    LatencySink(SinkFormatPtr sinkFormat,
                uint32_t numOfProducers,
                QueryId queryId,
                QuerySubPlanId querySubPlanId,
                std::chrono::milliseconds allowedDelay)
        : SinkMedium(std::move(sinkFormat), numOfProducers, queryId, querySubPlanId), allowedDelay(allowedDelay),
          numberOfProducers(numOfProducers) {}
    void setup() override;
    void shutdown() override {}
    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext& workerContext) override;

    std::string toString() const override { return "Latency Sink"; }
    SinkMediumTypes getSinkMediumType() override { return SinkMediumTypes::MONITORING_SINK; }
    uint32_t numberOfProducers;
};

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

inline void LatencySink::setup() {}
inline bool LatencySink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContext&) {
    using namespace std::chrono_literals;
    auto now = std::chrono::high_resolution_clock::now();
    auto sysclock_now = start + (now - start_hr);
    auto buffer_creation = std::chrono::time_point<std::chrono::system_clock>(
        std::chrono::microseconds(*reinterpret_cast<uint64_t*>(inputBuffer.getBuffer())));

    if (sysclock_now - buffer_creation > allowedDelay) {
        NES_WARNING("Timeout for origin {}: {}ms",
                    inputBuffer.getOriginId(),
                    std::chrono::duration_cast<std::chrono::milliseconds>(sysclock_now - buffer_creation).count());
    }

    return true;
}

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
