#include <iomanip>
#include <Sources/DataSource.hpp>
#include <Util/Logger.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <random>
#include <NodeEngine/NodeEngine.hpp>
#include <QueryCompiler/GeneratableOperators/TranslateToGeneratableOperatorPhase.hpp>
#include "../../tests/util/TestQuery.hpp"
#include "../../tests/util/DummySink.hpp"
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <filesystem>

#include <util/BenchmarkUtils.hpp>
#include <util/SimpleBenchmarkSource.hpp>
#include <util/SimpleBenchmarkSink.hpp>


using namespace NES;
int main() {

    std::cout << NES::BenchmarkUtils::getCurDateTimeStringWithNESVersion();

    std::vector<uint64_t> allIngestionRates;
    BenchmarkUtils::createRangeVector(allIngestionRates, 2 * 1000 * 1000, 10 * 1000 * 1000, 2 * 1000 * 1000);
    //BenchmarkUtils::createRangeVector(allIngestionRates, 10 * 1000 * 1000, 650 * 1000 * 1000, 10 * 1000 * 1000);

    std::vector<uint64_t> allExperimentsDuration;
    BenchmarkUtils::createRangeVector(allExperimentsDuration, 10, 20, 10);

    std::vector<uint64_t> allPeriodLengths;
    BenchmarkUtils::createRangeVector(allPeriodLengths, 1, 2, 1);


    std::string benchmarkFolderName = BenchmarkUtils::getCurDateTimeStringWithNESVersion();
    if (!std::filesystem::create_directory(benchmarkFolderName)) throw RuntimeException("Could not create folder " + benchmarkFolderName);

    //-----------------------------------------Start of BM_SimpleFilterQuery----------------------------------------------------------------------------------------------
    std::vector<uint64_t> allSelectivities;
    BenchmarkUtils::createRangeVector(allSelectivities, 500, 600, 100);

    for (auto selectivity : allSelectivities){
        auto benchmarkSchema = Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16);
        BM_AddBenchmark("BM_SimpleFilterQuery",
                        TestQuery::from(thisSchema).filter(Attribute("key") < selectivity).sink(DummySink::create()),
                        1,
                        SimpleBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), benchmarkSchema, ingestionRate),
                        SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                        "Selectivity",
                        "," + std::to_string(selectivity))

    }

    //-----------------------------------------End of BM_SimpleFilterQuery-----------------------------------------------------------------------------------------------

    //-----------------------------------------Start of BM_SimpleMapQuery----------------------------------------------------------------------------------------------

    auto benchmarkSchema = Schema::create()->addField("key", BasicType::INT16)->addField("value", BasicType::INT16);
    BM_AddBenchmark("BM_SimpleMapQuery",
                    TestQuery::from(thisSchema).map(Attribute("value") = Attribute("key") + Attribute("value")).sink(DummySink::create()),
                    1,
                    SimpleBenchmarkSource::create(nodeEngine->getBufferManager(), nodeEngine->getQueryManager(), benchmarkSchema, ingestionRate),
                    SimpleBenchmarkSink::create(benchmarkSchema, nodeEngine->getBufferManager()),
                    "",
                    "")
    //-----------------------------------------End of BM_SimpleMapQuery-----------------------------------------------------------------------------------------------


    return 0;
}