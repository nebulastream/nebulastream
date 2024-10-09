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

#include <fstream>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <argparse/argparse.hpp>
#include <google/protobuf/text_format.h>
#include <grpc++/create_channel.h>
#include <Configuration.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <SystestRunner.hpp>

using namespace std::literals;

class GRPCClient
{
public:
    explicit GRPCClient(std::shared_ptr<grpc::Channel> channel) : stub(WorkerRPCService::NewStub(channel)) { }
    std::unique_ptr<WorkerRPCService::Stub> stub;

    size_t registerQuery(const std::shared_ptr<NES::DecomposedQueryPlan> plan) const
    {
        grpc::ClientContext context;
        RegisterQueryReply reply;
        RegisterQueryRequest request;
        NES::DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, request.mutable_decomposedqueryplan());
        auto status = stub->RegisterQuery(&context, request, &reply);
        if (status.ok())
        {
            NES_DEBUG("Registration was successful.");
        }
        else
        {
            NES_THROW_RUNTIME_ERROR(fmt::format(
                "Registration failed. Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details()));
        }
        return reply.queryid();
    }

    void start(size_t queryId) const
    {
        grpc::ClientContext context;
        StartQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(queryId);
        auto status = stub->StartQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Starting was successful.");
        }
        else
        {
            NES_THROW_RUNTIME_ERROR(fmt::format(
                "Registration failed. Status: {}\nMessage: {}\nDetail: {}",
                magic_enum::enum_name(status.error_code()),
                status.error_message(),
                status.error_details()));
        }
    }
};

namespace NES
{

enum class Command : uint8_t
{
    run,
    cache
};

std::tuple<Configuration::SystestConfiguration, Command> readConfiguration(int argc, const char** argv)
{
    using namespace argparse;
    ArgumentParser program("systest");

    program.add_argument("-d", "--debug").flag().help("dump the query plan and enable debug logging");

    ArgumentParser run("run");
    run.add_argument("-s", "--server").help("grpc uri, e.g., 127.0.0.1:8080").required();
    run.add_argument("-t", "--test").help("directly specified test file e.g., fliter.test or directory to lookup test files in.");

    ArgumentParser cache("cache");
    cache.add_argument("-i", "--input").help("e.g., fliter.test");
    cache.add_argument("-o", "--output").help("output folder for protobuf file");

    program.add_subparser(run);
    program.add_subparser(cache);

    program.parse_args(argc, argv);

    if (program.get<bool>("-d"))
    {
        Logger::setupLogging("systest.log", LogLevel::LOG_DEBUG);
    }

    auto config = Configuration::SystestConfiguration();
    Command com;
    if (program.is_subcommand_used("run"))
    {
        com = Command::run;

        auto& parser = program.at<ArgumentParser>("run");
        config.grpcAddressUri = parser.get<std::string>("-s");

        auto testFileDefintion = parser.get<std::string>("-t");
        if (std::filesystem::is_directory(testFileDefintion))
        {
            config.testsDiscoverDir = testFileDefintion;
        }
        else if (std::filesystem::is_regular_file(testFileDefintion))
        {
            config.directlySpecifiedTestsFiles = testFileDefintion;
        }
        else
        {
            NES_FATAL_ERROR("{} is not a file or directory.", testFileDefintion);
            std::exit(1);
        }
    }
    else if (program.is_subcommand_used("cache"))
    {
        com = Command::cache;

        auto& parser = program.at<ArgumentParser>("cache");

        config.cacheDir = parser.get<std::string>("-o");
        if (not std::filesystem::is_directory(config.cacheDir.getValue()))
        {
            NES_FATAL_ERROR("{} is not a directory.", config.cacheDir.getValue());
            std::exit(1);
        }

        auto testFileDefintion = parser.get<std::string>("-i");
        if (std::filesystem::is_directory(testFileDefintion))
        {
            config.testsDiscoverDir = testFileDefintion;
        }
        else if (std::filesystem::is_regular_file(testFileDefintion))
        {
            config.directlySpecifiedTestsFiles = testFileDefintion;
        }
        else
        {
            NES_FATAL_ERROR("{} is not a file or directory.", testFileDefintion);
            std::exit(1);
        }
    }
    else
    {
        NES_ERROR("No command specified. Exiting.");
        std::exit(0);
    }

    return {config, com};
}

std::vector<std::filesystem::path> discoverTestFiles(const Configuration::SystestConfiguration& config)
{
    std::vector<std::filesystem::path> testFiles;
    if (!config.directlySpecifiedTestsFiles.getValue().empty())
    {
        testFiles.emplace_back(config.directlySpecifiedTestsFiles.getValue());
    }
    else
    {
        auto testsDiscoverDir = config.testsDiscoverDir;
        auto testFileExtension = config.testFileExtension;

        for (const auto& entry : std::filesystem::directory_iterator(std::filesystem::path(testsDiscoverDir)))
        {
            if (entry.is_regular_file() && entry.path().extension() == testFileExtension)
            {
                testFiles.push_back(entry.path());
            }
        }
    }
    return testFiles;
}
}

int main(int argc, const char** argv)
{
    using namespace NES;

    try
    {
        Logger::setupLogging("systest.log", LogLevel::LOG_ERROR);

        auto [config, com] = readConfiguration(argc, argv);

        DecomposedQueryPlanPtr decomposedQueryPlan;

        if (com == Command::run)
        {
            auto serverUri = config.grpcAddressUri;
            GRPCClient client(grpc::CreateChannel(serverUri, grpc::InsecureChannelCredentials()));

            auto testFiles = discoverTestFiles(config);

            for (const auto& testFile : testFiles)
            {
                /// Get from input the filename without the extension
                auto testname = std::filesystem::path(testFile).filename().string();

                auto decomposedQueryPlans = loadFromSLTFile(testFile, testname);
                for (std::size_t testnr = 0; const auto& plan : decomposedQueryPlans)
                {
                    SerializableDecomposedQueryPlan serialized;
                    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, &serialized);

                    GRPCClient client(grpc::CreateChannel(serverUri, grpc::InsecureChannelCredentials()));
                    auto queryId = client.registerQuery(plan);
                    client.start(queryId);
                    ++testnr;
                }
            }
        }
        else if (com == Command::cache)
        {
            auto testFiles = discoverTestFiles(config);

            for (const auto& testFile : testFiles)
            {
                /// Get from input the filename without the extension
                auto testname = testFile.stem().string();
                auto cacheDir = config.cacheDir.getValue();

                NES_INFO("Creating cache for {}", testname)

                /// A SqlLogicTest format file might have >=1 tests
                auto decomposedQueryPlans = loadFromSLTFile(testFile, testname);
                for (std::size_t testnr = 0; const auto& plan : decomposedQueryPlans)
                {
                    SerializableDecomposedQueryPlan serialized;
                    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, &serialized);
                    auto outfilename = fmt::format("{}/{}_{}.pb", cacheDir, testname, testnr);
                    std::ofstream file;
                    file = std::ofstream(outfilename);
                    if (!file)
                    {
                        NES_FATAL_ERROR("Could not open output file: {}", outfilename);
                        std::exit(1);
                    }
                    if (!serialized.SerializeToOstream(&file))
                    {
                        NES_FATAL_ERROR("Failed to write message to file.");
                        std::exit(1);
                    }
                    ++testnr;
                }
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException();
        std::exit(1);
    }
}
