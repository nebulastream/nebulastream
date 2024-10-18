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

#include <filesystem>
#include <regex>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <NebuLI.hpp>
#include <SLTParser.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SystestGrpc.hpp>
#include <SystestHelper.hpp>
#include <SystestRunner.hpp>

namespace NES::Systest
{
std::vector<Query> collectQueriesToTest(const Configuration::SystestConfiguration &configuration)
{
    auto testFiles = discoverTestFiles(configuration);

    std::vector<Query> queries{};
    for (const auto& testFile : testFiles)
    {
        auto testname = testFile.path.stem().string();

        std::vector<DecomposedQueryPlanPtr> loadedPlans{};
        auto cacheDir = configuration.cacheDir.getValue();
        if (configuration.useCachedQueries.getValue())
        {
            auto cacheFiles = getMatchingTestFiles(testname, configuration);
            auto serializedPlans = loadFromCacheFiles(cacheFiles.first);
            INVARIANT(cacheFiles.second.size() == serializedPlans.size(), "expect equal sizes")
            for (uint64_t i = 0; i < serializedPlans.size(); i++)
            {
                queries.emplace_back(
                    testname,
                    DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(&serializedPlans[i]),
                    cacheFiles.second[i],
                    std::nullopt);
            }
        }
        else
        {
            loadedPlans = loadFromSLTFile(testFile.path, configuration.resultDir.getValue(), testname);
            uint64_t queryNrInFile = 0;
            for (const auto& plan : loadedPlans)
            {
                if (not configuration.testNumbers.empty()) /// if specified, only run specific test numbers
                {
                    for (const auto& testNumber : configuration.testNumbers.getValues())
                    {
                        if (testNumber.getValue() == queryNrInFile + 1)
                        {
                            queries.emplace_back(testname, plan, queryNrInFile);
                        }
                    }
                }
                else
                {
                    queries.emplace_back(testname, plan, queryNrInFile);
                }
                queryNrInFile++;
            }
        }
    }
    return queries;
}

std::vector<DecomposedQueryPlanPtr>
loadFromSLTFile(const std::filesystem::path& testFilePath, const std::filesystem::path& resultDir, const std::string& testname)
{
    std::vector<DecomposedQueryPlanPtr> plans{};
    CLI::QueryConfig config{};
    SLTParser::SLTParser parser{};

    parser.registerSubstitutionRule(
        {"SINK",
         [&](std::string& substitute)
         {
             static uint64_t queryNr = 0;
             auto resultFile = resultDir.string() + "/" + testname + std::to_string(queryNr++) + ".csv";
             substitute = "sink(FileSinkDescriptor::create(\"" + resultFile + "\", \"CSV_FORMAT\", \"OVERWRITE\"));";
         }});

    parser.registerSubstitutionRule(
        {"TESTDATA", [&](std::string& substitute) { substitute = std::string(PATH_TO_BINARY_DIR) + "/test/testdata"; }});

    if (!parser.loadFile(testFilePath))
    {
        NES_FATAL_ERROR("Failed to parse test file: {}", testFilePath);
        return {};
    }

    /// We add new found sources to our config
    parser.registerOnCSVSourceCallback(
        [&](SLTParser::SLTParser::CSVSource&& source)
        {
            config.logical.emplace_back(CLI::LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<CLI::SchemaField> schema;
                    for (const auto& field : source.fields)
                    {
                        schema.push_back({.name = field.name, .type = field.type});
                    }
                    return schema;
                }()});

            config.physical.emplace_back(
                CLI::PhysicalSource{.logical = source.name, .config = {{"type", "CSV_SOURCE"}, {"filePath", source.csvFilePath}}});
        });

    const auto tmpSourceDir = std::string(PATH_TO_BINARY_DIR) + "/nes-systests/";
    parser.registerOnSLTSourceCallback(
        [&](SLTParser::SLTParser::SLTSource&& source)
        {
            static uint64_t sourceIndex = 0;

            config.logical.emplace_back(CLI::LogicalSource{
                .name = source.name,
                .schema = [&source]()
                {
                    std::vector<CLI::SchemaField> schema;
                    for (const auto& field : source.fields)
                    {
                        schema.push_back({.name = field.name, .type = field.type});
                    }
                    return schema;
                }()});

            config.physical.emplace_back(CLI::PhysicalSource{
                .logical = source.name,
                .config = {{"type", "CSV_SOURCE"}, {"filePath", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv"}}});


            /// Write the tuples to a tmp file
            std::ofstream sourceFile(tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
            if (!sourceFile)
            {
                NES_FATAL_ERROR("Failed to open source file: {}", tmpSourceDir + testname + std::to_string(sourceIndex) + ".csv");
                return;
            }

            /// Write tuples to csv file
            for (const auto& tuple : source.tuples)
            {
                sourceFile << tuple << std::endl;
            }
        });

    /// We create a new query plan from our config when finding a query
    parser.registerOnQueryCallback(
        [&](SLTParser::SLTParser::Query&& query)
        {
            config.query = query;
            try
            {
                auto plan = CLI::createFullySpecifiedQueryPlan(config);
                plans.emplace_back(plan);
            }
            catch (const std::exception& e)
            {
                NES_FATAL_ERROR("Failed to create query plan: {}", e.what());
                std::exit(-1);
            }
        });
    try
    {
        parser.parse();
    }
    catch (const Exception&)
    {
        tryLogCurrentException();
        return {};
    }
    return plans;
}

std::vector<SerializableDecomposedQueryPlan> loadFromCacheFiles(const std::vector<std::filesystem::path>& cacheFiles)
{
    std::vector<SerializableDecomposedQueryPlan> plans{};
    for (const auto& cacheFile : cacheFiles)
    {
        SerializableDecomposedQueryPlan queryPlan;
        std::ifstream file(cacheFile);
        if (!file || !queryPlan.ParseFromIstream(&file))
        {
            NES_ERROR("Could not load protobuffer file: {}", cacheFile);
        }
        else
        {
            std::cout << "loaded cached file:" << cacheFile << "\n";
            plans.emplace_back(queryPlan);
        }
    }
    return plans;
}

}
