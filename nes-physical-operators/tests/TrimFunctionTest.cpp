#include <memory>

#include <simdjson.h>


#include <gtest/gtest.h>
#include "DataTypes/DataTypeProvider.hpp"
#include "Functions/ArithmeticalFunctions/AddPhysicalFunction.hpp"
#include "Functions/ArithmeticalFunctions/DivPhysicalFunction.hpp"
#include "Functions/ConstantValuePhysicalFunction.hpp"
#include "Functions/ConstantValueVariableSizePhysicalFunction.hpp"
#include "Functions/FieldAccessLogicalFunction.hpp"
#include "Functions/FieldAccessPhysicalFunction.hpp"

#include <Engine.hpp>

#include "EmitOperatorHandler.hpp"
#include "EmitPhysicalOperator.hpp"
#include "MapPhysicalOperator.hpp"
#include "ScanPhysicalOperator.hpp"

namespace NES
{

/// The string view is valid as long as the underlying memory is valid. For this test the lifetime is tied to the Arena
std::string_view fromVarSized(int8_t* ptrToVarSized)
{
    auto data = ptrToVarSized + 4;
    auto size = *reinterpret_cast<uint32_t*>(ptrToVarSized);
    return std::string_view(reinterpret_cast<char*>(data), size);
}

/// black magic to convert the variable size data to a string_view
ConstantValueVariableSizePhysicalFunction input(std::string_view sv)
{
    return ConstantValueVariableSizePhysicalFunction(reinterpret_cast<const int8_t*>(sv.data()), sv.size());
}

struct MockPipelineExecutionContext : PipelineExecutionContext
{
public:
    bool emitBuffer(const TupleBuffer& buffer, ContinuationPolicy) override
    {
        emittedBuffers.emplace_back(buffer);
        return true;
    }

    void repeatTask(const TupleBuffer&, std::chrono::milliseconds) override { throw std::runtime_error("Not implemented"); }

    TupleBuffer allocateTupleBuffer() override { return bm->getBufferBlocking(); }

    [[nodiscard]] WorkerThreadId getId() const override { return WorkerThreadId(0); }

    [[nodiscard]] uint64_t getNumberOfWorkerThreads() const override { return 1; }

    [[nodiscard]] std::shared_ptr<AbstractBufferProvider> getBufferManager() const override { return bm; }

    [[nodiscard]] PipelineId getPipelineId() const override { return PipelineId(0); }

    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& getOperatorHandlers() override { return *handlers; }

    void setOperatorHandlers(std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>& handlers) override
    {
        this->handlers = &handlers;
    }

    explicit MockPipelineExecutionContext(const std::shared_ptr<AbstractBufferProvider>& bm) : bm(bm) { }

    std::shared_ptr<AbstractBufferProvider> bm;
    std::vector<TupleBuffer> emittedBuffers;
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>>* handlers = nullptr;
};

TEST(PhysicalFunction, BasicTest)
{
    auto bm = BufferManager::create();
    Arena arena{bm};

    nautilus::engine::Options options;
    /// Switch between compiled and interpreted
    options.setOption("engine.Compilation", true);
    options.setOption("dump.all", true);
    options.setOption("dump.console", false);
    options.setOption("dump.file", true);

    /**
     *
    *    PhysicalOperator(NES::ScanPhysicalOperator)
          PhysicalOperator(NES::MapPhysicalOperator)
            PhysicalOperator(NES::MapPhysicalOperator)
              PhysicalOperator(NES::MapPhysicalOperator)
                PhysicalOperator(NES::MapPhysicalOperator)
                  PhysicalOperator(NES::MapPhysicalOperator)
                    PhysicalOperator(NES::MapPhysicalOperator)
                      PhysicalOperator(NES::MapPhysicalOperator)
                        PhysicalOperator(NES::MapPhysicalOperator)
                          PhysicalOperator(NES::EmitPhysicalOperator)
     */

    nautilus::engine::NautilusEngine engine(options);

    /*
    f32 / i16 AS f32_i16,
    f32 / (i16 + INT16(1)) AS f32_i16_plus_1,
    f32 / i8 AS f32_i8,
    f32 / (i8 + INT8(1)) AS f32_i8_plus_1,
    f32 / i32 AS f32_i32,
    f32 / (i32 + INT32(1)) AS f32_i32_plus_1,
    f32 / i64 AS f32_i64,
    f32 / (i64 + INT64(1)) AS f32_i64_plus_1
*/
    /// f32 / i8 AS f32_i8,
    MapPhysicalOperator map_f32_i8(
        "f32_i8",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i8")))));
    /// f32 / i16 AS f32_i16
    MapPhysicalOperator map_f32_i16(
        "f32_i16",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i16")))));

    /// f32 / (i16 + INT16(1)) AS f32_i16_plus_1
    MapPhysicalOperator map_f32_i16_plus_1(
        "f32_i16_plus_1",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(
                    NES::AddPhysicalFunction(
                        NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i16")),
                        NES::PhysicalFunction(NES::ConstantInt16ValueFunction(1)))))));

    /// f32 / (i8 + INT8(1)) AS f32_i8_plus_1
    MapPhysicalOperator map_f32_i8_plus_1(
        "f32_i8_plus_1",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(
                    NES::AddPhysicalFunction(
                        NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i8")),
                        NES::PhysicalFunction(NES::ConstantInt8ValueFunction(1)))))));

    /// f32 / i32 AS f32_i32
    MapPhysicalOperator map_f32_i32(
        "f32_i32",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i32")))));

    /// f32 / (i32 + INT32(1)) AS f32_i32_plus_1
    MapPhysicalOperator map_f32_i32_plus_1(
        "f32_i32_plus_1",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(
                    NES::AddPhysicalFunction(
                        NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i32")),
                        NES::PhysicalFunction(NES::ConstantInt32ValueFunction(1)))))));

    /// f32 / i64 AS f32_i64
    MapPhysicalOperator map_f32_i64(
        "f32_i64",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i64")))));

    /// f32 / (i64 + INT64(1)) AS f32_i64_plus_1
    MapPhysicalOperator map_f32_i64_plus_1(
        "f32_i64_plus_1",
        NES::PhysicalFunction(
            NES::DivPhysicalFunction(
                NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("f32")),
                NES::PhysicalFunction(
                    NES::AddPhysicalFunction(
                        NES::PhysicalFunction(NES::FieldAccessPhysicalFunction("i64")),
                        NES::PhysicalFunction(NES::ConstantInt64ValueFunction(1)))))));


    ///  CREATE LOGICAL SOURCE stream4(i8 INT8, i16 INT16, i32 INT32, i64 INT64, f32 FLOAT32, f64 FLOAT64);
    auto tbref = TupleBufferRef::create(
        4096,
        Schema{}
            .addField("i8", DataTypeProvider::provideDataType("INT8"))
            .addField("i16", DataTypeProvider::provideDataType("INT16"))
            .addField("i32", DataTypeProvider::provideDataType("INT32"))
            .addField("i64", DataTypeProvider::provideDataType("INT64"))
            .addField("f32", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f64", DataTypeProvider::provideDataType("FLOAT64")));

    auto tbrefEmit = TupleBufferRef::create(
        4096,
        Schema{}
            .addField("f32_i16", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i16_plus_1", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i8", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i8_plus_1", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i32", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i32_plus_1", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i64", DataTypeProvider::provideDataType("FLOAT32"))
            .addField("f32_i64_plus_1", DataTypeProvider::provideDataType("FLOAT32")));


    auto op
        = PhysicalOperator(map_f32_i16)
              .withChild(
                  PhysicalOperator(map_f32_i16_plus_1)
                      .withChild(
                          PhysicalOperator(map_f32_i8)
                              .withChild(
                                  PhysicalOperator(map_f32_i8_plus_1)
                                      .withChild(
                                          PhysicalOperator(map_f32_i32)
                                              .withChild(PhysicalOperator(map_f32_i32_plus_1)
                                                             .withChild(PhysicalOperator(map_f32_i64)
                                                                            .withChild(PhysicalOperator(map_f32_i64_plus_1)
                                                                                           .withChild(PhysicalOperator(EmitPhysicalOperator(
                                                                                               OperatorHandlerId(0), tbrefEmit))))))))));

    auto function = engine.registerFunction(
        std::function(
            [&](nautilus::val<int8_t> i8,
                nautilus::val<int16_t> i16,
                nautilus::val<int32_t> i32,
                nautilus::val<int64_t> i64,
                nautilus::val<float> f32,
                nautilus::val<double> f64,
                nautilus::val<TupleBuffer*> input,
                nautilus::val<PipelineExecutionContext*> pec)
            {
                Record r{
                    {{"i8", VarVal(i8)},
                     {"i16", VarVal(i16)},
                     {"i32", VarVal(i32)},
                     {"i64", VarVal(i64)},
                     {"f32", VarVal(f32)},
                     {"f64", VarVal(f64)}}};
                RecordBuffer recordBuffer{input};
                ExecutionContext ec{pec, &arena};
                ec.chunkNumber = INITIAL<ChunkNumber>;
                ec.sequenceNumber = INITIAL<SequenceNumber>;

                op.open(ec, recordBuffer);
                op.execute(ec, r);
                op.close(ec, recordBuffer);
            }));


    auto handler = std::make_unique<EmitOperatorHandler>();
    std::unordered_map<OperatorHandlerId, std::shared_ptr<OperatorHandler>> handlers;
    handlers.insert({OperatorHandlerId(0), std::move(handler)});
    MockPipelineExecutionContext pec{bm};
    pec.setOperatorHandlers(handlers);
    auto inputBuffer = bm->getBufferBlocking();
    inputBuffer.setChunkNumber(INITIAL<ChunkNumber>);
    inputBuffer.setNumberOfTuples(1);
    inputBuffer.setSequenceNumber(INITIAL<SequenceNumber>);

    nautilus::val<uint64_t> index(0);

    /// i8 INT8, i16 INT16, i32 INT32, i64 INT64, f32 FLOAT32, f64 FLOAT64
    /// -42,-129,-32769,-2147483649,23.0,23.0
    tbref->writeRecord(
        index,
        RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(inputBuffer))},
        Record{std::unordered_map<Record::RecordFieldIdentifier, VarVal>{
            {"i8", VarVal(nautilus::val<int8_t>(-42))},
            {"i16", VarVal(nautilus::val<int16_t>(-129))},
            {"i32", VarVal(nautilus::val<int32_t>(-32769))},
            {"i64", VarVal(nautilus::val<int64_t>(-2147483649))},
            {"f32", VarVal(nautilus::val<float>(23.0f))},
            {"f64", VarVal(nautilus::val<double>(23.0))}}},
        nautilus::val<AbstractBufferProvider*>(bm.get()));

    pec.setOperatorHandlers(handlers);
    function(1, 1, 1, 1, 2.0f, 23.0, std::addressof(inputBuffer), std::addressof(pec));

    ASSERT_EQ(pec.emittedBuffers.size(), 1);
    auto& outputBuffer = pec.emittedBuffers[0];

    /// -0.178295 -0.179687 -0.547619 -0.560976 -0.000702 -0.000702 -0.0 -0.0
    auto record_f32_i16
        = tbrefEmit->readRecord({"f32_i16"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i16
        = nautilus::details::RawValueResolver<float>().getRawValue(record_f32_i16.read("f32_i16").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i16, 2, 1e-5);

    auto record_f32_i16_plus_1
        = tbrefEmit->readRecord({"f32_i16_plus_1"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i16_plus_1 = nautilus::details::RawValueResolver<float>().getRawValue(
        record_f32_i16_plus_1.read("f32_i16_plus_1").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i16_plus_1, 1, 1e-5);

    auto record_f32_i8 = tbrefEmit->readRecord({"f32_i8"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i8 = nautilus::details::RawValueResolver<float>().getRawValue(record_f32_i8.read("f32_i8").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i8, 2, 1e-5);

    auto record_f32_i8_plus_1
        = tbrefEmit->readRecord({"f32_i8_plus_1"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i8_plus_1
        = nautilus::details::RawValueResolver<float>().getRawValue(record_f32_i8_plus_1.read("f32_i8_plus_1").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i8_plus_1, 1, 1e-5);

    auto record_f32_i32
        = tbrefEmit->readRecord({"f32_i32"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i32
        = nautilus::details::RawValueResolver<float>().getRawValue(record_f32_i32.read("f32_i32").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i32, 2, 1e-5);

    auto record_f32_i32_plus_1
        = tbrefEmit->readRecord({"f32_i32_plus_1"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i32_plus_1 = nautilus::details::RawValueResolver<float>().getRawValue(
        record_f32_i32_plus_1.read("f32_i32_plus_1").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i32_plus_1, 1, 1e-5);

    auto record_f32_i64
        = tbrefEmit->readRecord({"f32_i64"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i64
        = nautilus::details::RawValueResolver<float>().getRawValue(record_f32_i64.read("f32_i64").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i64, 2, 1e-5);

    auto record_f32_i64_plus_1
        = tbrefEmit->readRecord({"f32_i64_plus_1"}, RecordBuffer{nautilus::val<TupleBuffer*>(std::addressof(outputBuffer))}, index);
    auto val_f32_i64_plus_1 = nautilus::details::RawValueResolver<float>().getRawValue(
        record_f32_i64_plus_1.read("f32_i64_plus_1").cast<nautilus::val<float>>());
    EXPECT_NEAR(val_f32_i64_plus_1, 1, 1e-5);
}
}
