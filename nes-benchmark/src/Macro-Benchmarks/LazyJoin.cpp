/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Catalogs/Source/PhysicalSource.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/CSVSourceType.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/MemorySourceType.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Runtime/Execution/ExecutablePipelineStage.hpp>
#include <Runtime/Execution/PipelineExecutionContext.hpp>
#include <Runtime/HardwareManager.hpp>
#include <Runtime/NesThread.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/NodeEngineFactory.hpp>
#include <Runtime/WorkerContext.hpp>
#include <Sinks/SinkCreator.hpp>
#include <Sources/SourceCreator.hpp>
#include <random>
#include <tsl/robin_map.h>

#ifndef min
#define min(A, B) ((A) < (B) ? (A) : (B))
#endif

struct LeftStream {
    uint64_t key;
    uint64_t timestamp;
};

struct RightStream {
    uint64_t key;
    uint64_t timestamp;
};

namespace Slash {
class abstract_zipfian_generator {
  public:
    virtual uint64_t operator()(std::mt19937& rng) = 0;
};

class ycsb_zipfian_generator : public abstract_zipfian_generator {
    static constexpr auto default_zipfian_factor = .99;

  public:
    explicit ycsb_zipfian_generator(uint64_t min_, uint64_t max_, double zipfian_factor = default_zipfian_factor)
        : ycsb_zipfian_generator(min_, max_, zipfian_factor, compute_zeta(0, max_ - min_ + 1, zipfian_factor, 0)) {
        // nop
    }

    explicit ycsb_zipfian_generator(uint64_t min_, uint64_t max_, double zipfianconstant_, double zetan_) : dist(0.0, 1.0) {
        num_items = max_ - min_ + 1;
        min = min_;
        theta = zipfianconstant = zipfianconstant_;

        theta = zipfianconstant;

        zeta2theta = zeta(0, 2, theta, 0);

        alpha = 1.0 / (1.0 - theta);
        zetan = zetan_;
        countforzeta = num_items;
        eta = (1 - std::pow(2.0 / num_items, 1 - theta)) / (1 - zeta2theta / zetan);
    }

    uint64_t operator()(std::mt19937& rng) override { return (*this)(rng, countforzeta); }

    uint64_t operator()(std::mt19937& rng, uint64_t new_item_count) {
        if (new_item_count > countforzeta) {
            // we have added more items. can compute zetan incrementally, which is cheaper
            num_items = new_item_count;
            zetan = zeta(countforzeta, num_items, theta, zetan);
            eta = (1 - std::pow(2.0 / num_items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
        double u = dist(rng);
        double uz = u * zetan;
        if (uz < 1.0) {
            return min;
        }

        if (uz < 1.0 + std::pow(0.5, theta)) {
            return min + 1;
        }

        long ret = min + (long) ((num_items) *std::pow(eta * u - eta + 1, alpha));
        return ret;
    }

  private:
    double zeta(uint64_t st, uint64_t n, double thetaVal, double initialsum) {
        countforzeta = n;
        return compute_zeta(st, n, thetaVal, initialsum);
    }

    static double compute_zeta(uint64_t st, uint64_t n, double theta, double initialsum) {
        double sum = initialsum;
        for (auto i = st; i < n; i++) {
            sum += 1 / (std::pow(i + 1, theta));
        }
        return sum;
    }

  private:
    uint64_t num_items;
    uint64_t min;
    double zipfianconstant;
    double alpha, zetan, eta, theta, zeta2theta;
    uint64_t countforzeta;
    std::uniform_real_distribution<double> dist;
};
}// namespace Slash

namespace detail {
int memcpy_uncached_store_avx(void* dest, const void* src, size_t n_bytes) {
    int ret = 0;
#ifdef __AVX__
    char* d = (char*) dest;
    uintptr_t d_int = (uintptr_t) d;
    const char* s = (const char*) src;
    uintptr_t s_int = (uintptr_t) s;
    size_t n = n_bytes;

    // align dest to 256-bits
    if (d_int & 0x1f) {
        size_t nh = min(0x20 - (d_int & 0x1f), n);
        memcpy(d, s, nh);
        d += nh;
        d_int += nh;
        s += nh;
        s_int += nh;
        n -= nh;
    }

    if (s_int & 0x1f) {// src is not aligned to 256-bits
        __m256d r0, r1, r2, r3;
        // unroll 4
        while (n >= 4 * sizeof(__m256d)) {
            r0 = _mm256_loadu_pd((double*) (s + 0 * sizeof(__m256d)));
            r1 = _mm256_loadu_pd((double*) (s + 1 * sizeof(__m256d)));
            r2 = _mm256_loadu_pd((double*) (s + 2 * sizeof(__m256d)));
            r3 = _mm256_loadu_pd((double*) (s + 3 * sizeof(__m256d)));
            _mm256_stream_pd((double*) (d + 0 * sizeof(__m256d)), r0);
            _mm256_stream_pd((double*) (d + 1 * sizeof(__m256d)), r1);
            _mm256_stream_pd((double*) (d + 2 * sizeof(__m256d)), r2);
            _mm256_stream_pd((double*) (d + 3 * sizeof(__m256d)), r3);
            s += 4 * sizeof(__m256d);
            d += 4 * sizeof(__m256d);
            n -= 4 * sizeof(__m256d);
        }
        while (n >= sizeof(__m256d)) {
            r0 = _mm256_loadu_pd((double*) (s));
            _mm256_stream_pd((double*) (d), r0);
            s += sizeof(__m256d);
            d += sizeof(__m256d);
            n -= sizeof(__m256d);
        }
    } else {// or it IS aligned
        __m256d r0, r1, r2, r3, r4, r5, r6, r7;
        // unroll 8
        while (n >= 8 * sizeof(__m256d)) {
            r0 = _mm256_load_pd((double*) (s + 0 * sizeof(__m256d)));
            r1 = _mm256_load_pd((double*) (s + 1 * sizeof(__m256d)));
            r2 = _mm256_load_pd((double*) (s + 2 * sizeof(__m256d)));
            r3 = _mm256_load_pd((double*) (s + 3 * sizeof(__m256d)));
            r4 = _mm256_load_pd((double*) (s + 4 * sizeof(__m256d)));
            r5 = _mm256_load_pd((double*) (s + 5 * sizeof(__m256d)));
            r6 = _mm256_load_pd((double*) (s + 6 * sizeof(__m256d)));
            r7 = _mm256_load_pd((double*) (s + 7 * sizeof(__m256d)));
            _mm256_stream_pd((double*) (d + 0 * sizeof(__m256d)), r0);
            _mm256_stream_pd((double*) (d + 1 * sizeof(__m256d)), r1);
            _mm256_stream_pd((double*) (d + 2 * sizeof(__m256d)), r2);
            _mm256_stream_pd((double*) (d + 3 * sizeof(__m256d)), r3);
            _mm256_stream_pd((double*) (d + 4 * sizeof(__m256d)), r4);
            _mm256_stream_pd((double*) (d + 5 * sizeof(__m256d)), r5);
            _mm256_stream_pd((double*) (d + 6 * sizeof(__m256d)), r6);
            _mm256_stream_pd((double*) (d + 7 * sizeof(__m256d)), r7);
            s += 8 * sizeof(__m256d);
            d += 8 * sizeof(__m256d);
            n -= 8 * sizeof(__m256d);
        }
        while (n >= sizeof(__m256d)) {
            r0 = _mm256_load_pd((double*) (s));
            _mm256_stream_pd((double*) (d), r0);
            s += sizeof(__m256d);
            d += sizeof(__m256d);
            n -= sizeof(__m256d);
        }
    }

    if (n)
        memcpy(d, s, n);

    // fencing is needed even for plain memcpy(), due to performance
    // being hit by delayed flushing of WC buffers
    _mm_sfence();

#else
#error "this file should be compiled with -mavx"
#endif
    return ret;
}

template<typename T = void>
T* allocAligned(size_t size, size_t alignment = 16) {
    void* tmp = nullptr;
    NES_ASSERT(0 == posix_memalign(&tmp, alignment, sizeof(T) * size), "Cannot allocate");
    return reinterpret_cast<T*>(tmp);
}

}// namespace detail

template<typename T>
class FixedPagesLinkedList {
  public:

    explicit FixedPagesLinkedList(size_t size, size_t alignment) {
        data = detail::allocAligned<T*>(size, alignment);
    }

    ~FixedPagesLinkedList() {
        free(data);
        data = nullptr;
    }

    void append(const T* element) {
        //        detail::memcpy_uncached_store_avx(&data, element, sizeof(T));
        data = *element;
    }

  private:
    T** data;
};

namespace NES {
template<typename T>
class JoinPipelineStage : public Runtime::Execution::ExecutablePipelineStage {
  public:
    explicit JoinPipelineStage(PipelineStageArity arity)
        : Runtime::Execution::ExecutablePipelineStage(arity) {}//, joinState(std::move(joinState)) {}

    ExecutionResult
    execute(Runtime::TupleBuffer& buffer, Runtime::Execution::PipelineExecutionContext&, Runtime::WorkerContext& wctx) override {
        auto* input = buffer.getBuffer<T>();
        auto numOfTuples = buffer.getNumberOfTuples();

        auto& ht = this->ht[wctx.getId()].tbl;

        for (uint32_t i = 0; i < numOfTuples; ++i) {
            auto* record = input + i;
            ht[record->key].append(record);
        }

        return ExecutionResult::Ok;
    }

    void reconfigure(Runtime::ReconfigurationMessage& message, Runtime::WorkerContext& context) override {
        Reconfigurable::reconfigure(message, context);
        ht[context.getId()].tbl.reserve(1024);
    }

  private:
    struct alignas(64) HashTable {
        tsl::robin_map<uint64_t, FixedPagesLinkedList<T>> tbl;
        //        std::array<T, 1024> tbl;
    };

    std::array<HashTable, Runtime::NesThread::MaxNumThreads> ht;
};

class LeftJoinPipelineStage : public JoinPipelineStage<LeftStream> {
  public:
    explicit LeftJoinPipelineStage() : JoinPipelineStage(BinaryLeft) {}
};

class RightJoinPipelineStage : public JoinPipelineStage<RightStream> {
  public:
    explicit RightJoinPipelineStage() : JoinPipelineStage(BinaryRight) {}
};

void createBenchmarkSources(std::vector<DataSourcePtr>& dataProducers,
                            Runtime::NodeEnginePtr nodeEngine,
                            SchemaPtr schema,
                            size_t partitionSizeBytes,
                            uint64_t numOfSources,
                            uint64_t startOffset,
                            Runtime::Execution::ExecutablePipelinePtr pipeline,
                            std::function<void(uint8_t*, size_t)> writer) {
    auto ofs = dataProducers.size() + startOffset;
    auto bufferSize = nodeEngine->getBufferManager()->getBufferSize();
    size_t numOfBuffers = partitionSizeBytes / bufferSize;
    for (auto i = 0ul; i < numOfSources; ++i) {
        size_t dataSegmentSize = bufferSize * numOfBuffers;
        auto* data = new uint8_t[dataSegmentSize];
        writer(data, dataSegmentSize);
        std::shared_ptr<uint8_t> ptr(data, [](uint8_t* ptr) {
            delete[] ptr;
        });
        std::vector<Runtime::Execution::SuccessorExecutablePipeline> pipelines;
        pipelines.emplace_back(pipeline);
        auto source = std::make_shared<BenchmarkSource>(schema,
                                                        std::move(ptr),
                                                        dataSegmentSize,
                                                        nodeEngine->getBufferManager(),
                                                        nodeEngine->getQueryManager(),
                                                        numOfBuffers,
                                                        0ul,
                                                        i + ofs,
                                                        64ul,
                                                        GatheringMode::FREQUENCY_MODE,
                                                        SourceMode::COPY_BUFFER,
                                                        i,
                                                        0ul,
                                                        pipelines);
        dataProducers.emplace_back(source);
    }
}
}// namespace NES
int main() {
    using namespace NES;
    NES::setupLogging("LazyJoin.log", NES::LOG_INFO);
    auto lhsStreamSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt64())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

    auto rhsStreamSchema = Schema::create()
                               ->addField("key", DataTypeFactory::createUInt64())
                               ->addField("timestamp", DataTypeFactory::createUInt64());

    std::vector<PhysicalSourcePtr> physicalSources;

    auto engine = Runtime::NodeEngineFactory::createNodeEngine("127.0.0.1",
                                                               0,
                                                               physicalSources,
                                                               8,
                                                               128 * 1024,
                                                               1024,
                                                               64,
                                                               64,
                                                               Configurations::QueryCompilerConfiguration());

    auto sink = createNullOutputSink(0, engine);

    std::vector<Runtime::Execution::OperatorHandlerPtr> operatorHandlers;

    //    auto joinStateLeft = std::make_shared<JoinState>(engine->getHardwareManager(), 1024, 1024 * 1024 * 1024);
    //    auto joinStateRight = std::make_shared<JoinState>(engine->getHardwareManager(), 1024, 1024 * 1024 * 1024);

    auto executionContextLeft = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
        0,
        engine->getQueryManager(),
        [sink](Runtime::TupleBuffer& buffer, Runtime::WorkerContext& wctx) {
            sink->writeData(buffer, wctx);
        },
        [](Runtime::TupleBuffer&) {
        },
        operatorHandlers);

    auto executionContextRight = std::make_shared<Runtime::Execution::PipelineExecutionContext>(
        0,
        engine->getQueryManager(),
        [sink](Runtime::TupleBuffer& buffer, Runtime::WorkerContext& wctx) {
            sink->writeData(buffer, wctx);
        },
        [](Runtime::TupleBuffer&) {
        },
        operatorHandlers);

    auto executableLeft = std::make_shared<LeftJoinPipelineStage>();
    auto executableRight = std::make_shared<RightJoinPipelineStage>();

    auto numSourcesLeft = 2;
    auto numSourcesRight = 2;

    auto pipelineLeft =
        Runtime::Execution::ExecutablePipeline::create(0, 0, executionContextLeft, executableLeft, numSourcesLeft, {sink});
    auto pipelineRight =
        Runtime::Execution::ExecutablePipeline::create(1, 0, executionContextRight, executableRight, numSourcesRight, {sink});

    std::vector<DataSourcePtr> dataProducers;

    createBenchmarkSources(dataProducers,
                           engine,
                           lhsStreamSchema,
                           128 * 1024 * 1024,
                           numSourcesLeft,
                           1,
                           pipelineLeft,
                           [](uint8_t* data, size_t length) {
                               auto* ptr = reinterpret_cast<LeftStream*>(data);
                               auto numOfTuples = length / sizeof(LeftStream);
                               std::mt19937 engine(std::chrono::high_resolution_clock::now().time_since_epoch().count());
                               Slash::ycsb_zipfian_generator generator(0, 100'000, 0.4);

                               for (size_t i = 0; i < numOfTuples; ++i) {
                                   ptr[i].key = generator(engine);
                                   ptr[i].timestamp = 0;
                               }
                           });

    createBenchmarkSources(dataProducers,
                           engine,
                           rhsStreamSchema,
                           128 * 1024 * 1024,
                           numSourcesRight,
                           1,
                           pipelineRight,
                           [](uint8_t* data, size_t length) {
                               auto* ptr = reinterpret_cast<RightStream*>(data);
                               auto numOfTuples = length / sizeof(RightStream);
                               std::mt19937 engine(std::chrono::high_resolution_clock::now().time_since_epoch().count());
                               Slash::ycsb_zipfian_generator generator(0, 100'000, 0.4);

                               for (size_t i = 0; i < numOfTuples; ++i) {
                                   ptr[i].key = generator(engine);
                                   ptr[i].timestamp = 0;
                               }
                           });

    auto executionPlan = Runtime::Execution::ExecutableQueryPlan::create(0,
                                                                         0,
                                                                         dataProducers,
                                                                         {sink},
                                                                         {pipelineLeft, pipelineRight},
                                                                         engine->getQueryManager(),
                                                                         engine->getBufferManager());

    NES_ASSERT(engine->registerQueryInNodeEngine(executionPlan), "Cannot register query");

    auto startTs = std::chrono::high_resolution_clock::now();

    NES_ASSERT(engine->startQuery(executionPlan->getQueryId()), "Cannot start query");

    while (engine->getQueryStatus(executionPlan->getQueryId()) == Runtime::Execution::ExecutableQueryPlanStatus::Running) {
        _mm_pause();
    }

    auto statistics = engine->getQueryManager()->getQueryStatistics(executionPlan->getQuerySubPlanId());
    auto nowTs = std::chrono::high_resolution_clock::now();
    auto elapsedNs = std::chrono::duration_cast<std::chrono::nanoseconds>(nowTs - startTs);
    double elapsedSec = elapsedNs.count() / 1'000'000'000.0;
    double MTuples = statistics->getProcessedTuple() / 1'000'000.0;
    auto throughputTuples = MTuples / elapsedSec;
    auto throughputMBytes = ((16ul * MTuples * 1'000'000.0) / elapsedSec) / (1024ul * 1024ul);
    NES_INFO("Processed: " << throughputTuples << " MTuple/sec - " << throughputMBytes << " MB/sec");

    NES_ASSERT(engine->undeployQuery(executionPlan->getQueryId()), "Cannot undeploy query");
    NES_ASSERT(engine->stop(), "Cannot stop query");

    //    auto query = Query::from("window1").joinWith(Query::from("window2")).where(Attribute("id1")).equalsTo(Attribute("id2")).window(TumblingWindow::of(EventTime(Attribute("timestamp")), Milliseconds(1000))));
    //
    //    TestHarness testHarness = TestHarness(query, restPort, rpcPort)
    //                                  .addLogicalSource("window1", windowSchema)
    //                                  .addLogicalSource("window2", window2Schema)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window1", csvSourceType1)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window1", csvSourceType1)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType2)
    //                                  .attachWorkerWithCSVSourceToCoordinator("window2", csvSourceType2)
    //                                  .validate()
    //                                  .setupTopology();

    struct Output {
        int64_t start;
        int64_t end;
        int64_t key;
        int64_t win1;
        uint64_t id1;
        uint64_t timestamp1;
        int64_t win2;
        uint64_t id2;
        uint64_t timestamp2;

        // overload the == operator to check if two instances are the same
        bool operator==(Output const& rhs) const {
            return (start == rhs.start && end == rhs.end && key == rhs.key && win1 == rhs.win1 && id1 == rhs.id1
                    && timestamp1 == rhs.timestamp1 && win2 == rhs.win2 && id2 == rhs.id2 && timestamp2 == rhs.timestamp2);
        }
    };

    std::vector<Output> expectedOutput = {{1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 4, 1, 4, 1002, 3, 4, 1102},    {1000, 2000, 4, 1, 4, 1002, 3, 4, 1112},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011}, {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011}, {1000, 2000, 12, 1, 12, 1001, 5, 12, 1011},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},    {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},    {2000, 3000, 1, 2, 1, 2000, 2, 1, 2010},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}, {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301},
                                          {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}, {2000, 3000, 11, 2, 11, 2001, 2, 11, 2301}};

    //    std::vector<Output> actualOutput = testHarness.getOutput<Output>(expectedOutput.size(), "TopDown", "NONE", "IN_MEMORY");
    return 0;
}