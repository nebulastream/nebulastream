#ifndef NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
#define NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_

#include <Sinks/Mediums/SinkMedium.hpp>
#include <Sinks/Formats/NesFormat.hpp>
#include <future>

namespace NES {
/**
 * @brief SimpleBenchmarkSink will set completed to true, after it gets @param expectedNumberOfTuples have been processed by SimpleBenchmarkSink
 */
class SimpleBenchmarkSink : public SinkMedium {
  public:
    SimpleBenchmarkSink(SchemaPtr schema, BufferManagerPtr bufferManager)
        : SinkMedium(std::make_shared<NesFormat>(schema, bufferManager)) {};

    static std::shared_ptr<SimpleBenchmarkSink> create(SchemaPtr schema,
                                                       BufferManagerPtr bufferManager) {
        return std::make_shared<SimpleBenchmarkSink>(schema, bufferManager);
    }

    bool writeData(TupleBuffer& input_buffer) override {
        std::unique_lock lock(m);
        NES_DEBUG("SimpleBenchmarkSink: got buffer with " << input_buffer.getNumberOfTuples() << " number of tuples!");

        currentTuples += input_buffer.getNumberOfTuples();

        NES_DEBUG("SimpleBenchmarkSink: currentTuples=" << currentTuples);

        return true;
    }

    SinkMediumTypes getSinkMediumType() override {
        return SinkMediumTypes::PRINT_SINK;
    }

    TupleBuffer& get(size_t index) {
        std::unique_lock lock(m);
        return resultBuffers[index];
    }

    const std::string toString() const override {
        return "";
    }

    void setup() override {};

    std::string toString() override {
        return "Test_Sink";
    }

    void shutdown() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    ~SimpleBenchmarkSink() override {
        std::unique_lock lock(m);
        cleanupBuffers();
    };

    uint32_t getNumberOfResultBuffers() {
        std::unique_lock lock(m);
        return resultBuffers.size();
    }

  private:
    void cleanupBuffers() {
        for (auto& buffer : resultBuffers) {
            buffer.release();
        }
        resultBuffers.clear();
    }

    uint64_t currentTuples = 0;
    std::vector<TupleBuffer> resultBuffers;
    std::mutex m;

  public:
    std::promise<bool> completed;

};
}

#endif //NES_BENCHMARK_INCLUDE_UTIL_SIMPLEBENCHMARKSINK_HPP_
