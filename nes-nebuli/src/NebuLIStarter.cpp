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
#include <Configurations/Coordinator/CoordinatorConfiguration.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Plans/Query/QueryPlan.hpp>
#include <argparse/argparse.hpp>
#include <google/protobuf/text_format.h>
#include <grpc++/create_channel.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>

#include <NebuLI.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

namespace NES
{
class TCPSourceType;
}
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

    void stop(size_t queryId) const
    {
        grpc::ClientContext context;
        StopQueryRequest request;
        request.set_queryid(queryId);
        request.set_terminationtype(StopQueryRequest::HardStop);
        google::protobuf::Empty response;
        auto status = stub->StopQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Stopping was successful.");
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

    void status(size_t queryId) const
    {
        grpc::ClientContext context;
        QuerySummaryRequest request;
        request.set_queryid(queryId);
        QuerySummaryReply response;
        auto status = stub->RequestQuerySummary(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Stopping was successful.");
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

    void unregister(size_t queryId) const
    {
        grpc::ClientContext context;
        UnregisterQueryRequest request;
        google::protobuf::Empty response;
        request.set_queryid(queryId);
        auto status = stub->UnregisterQuery(&context, request, &response);
        if (status.ok())
        {
            NES_DEBUG("Unregister was successful.");
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

int main(int argc, char** argv)
{
    using namespace NES;
    using namespace argparse;
    ArgumentParser program("nebuli");
    program.add_argument("-d", "--debug").flag().help("Dump the Query plan and enable debug logging");

    ArgumentParser registerQuery("register");
    registerQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");
    registerQuery.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");
    registerQuery.add_argument("-x").flag();

    ArgumentParser startQuery("start");
    startQuery.add_argument("queryId").scan<'i', size_t>();
    startQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");

    ArgumentParser stopQuery("stop");
    stopQuery.add_argument("queryId").scan<'i', size_t>();
    stopQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");

    ArgumentParser testQuery("test");
    testQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");
    testQuery.add_argument("-f", "--file").help("e.g., fliter.test");

    ArgumentParser unregisterQuery("unregister");
    unregisterQuery.add_argument("queryId").scan<'i', size_t>();
    unregisterQuery.add_argument("-s", "--server").help("grpc uri e.g., 127.0.0.1:8080");

    ArgumentParser dump("dump");
    dump.add_argument("-o", "--output").default_value("-").help("Write the DecomposedQueryPlan to file. Use - for stdout");
    dump.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");

    program.add_subparser(registerQuery);
    program.add_subparser(startQuery);
    program.add_subparser(stopQuery);
    program.add_subparser(testQuery);
    program.add_subparser(unregisterQuery);
    program.add_subparser(dump);

    try
    {
        program.parse_args(argc, argv);
    }
    catch (const std::exception& err)
    {
        std::cerr << err.what() << std::endl;
        std::cerr << program;
        std::exit(1);
    }

    if (program.get<bool>("-d"))
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_DEBUG);
    }
    else
    {
        NES::Logger::setupLogging("client.log", NES::LogLevel::LOG_ERROR);
    }

    bool handled = false;
    for (const auto& [name, fn] : std::initializer_list<std::pair<std::string_view, void (GRPCClient::*)(size_t) const>>{
             {"start", &GRPCClient::start}, {"unregister", &GRPCClient::unregister}, {"stop", &GRPCClient::stop}})
    {
        if (program.is_subcommand_used(name))
        {
            auto& parser = program.at<ArgumentParser>(name);
            auto serverUri = parser.get<std::string>("-s");
            GRPCClient client(CreateChannel(serverUri, grpc::InsecureChannelCredentials()));
            auto queryId = parser.get<size_t>("queryId");
            (client.*fn)(queryId);
            handled = true;
            break;
        }
    }

    if (handled)
    {
        return 0;
    }

    DecomposedQueryPlanPtr decomposedQueryPlan;
    try
    {
        if (program.is_subcommand_used("test"))
        {
            auto& testArgs = program.at<ArgumentParser>("test");
            auto serverUri = testArgs.get<std::string>("-s");
            auto testFile = testArgs.get<std::string>("-f");
            GRPCClient client(grpc::CreateChannel(serverUri, grpc::InsecureChannelCredentials()));

            /// Get from input the filename without the extension
            auto testname
                = std::filesystem::path(testFile).filename().string(); /// A SqlLogicTest format file might have more than one test

            auto decomposedQueryPlans = NES::CLI::loadFromSLTFile(testFile, testname);
            for (std::size_t testnr = 0; const auto& plan : decomposedQueryPlans)
            {
                SerializableDecomposedQueryPlan serialized;
                DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, &serialized);

                GRPCClient client(grpc::CreateChannel(serverUri, grpc::InsecureChannelCredentials()));
                auto queryId = client.registerQuery(plan);
                client.start(queryId);
                ++testnr;
            }
            return 0;
        }

        std::string const command = program.is_subcommand_used("register") ? "register" : "dump";
        auto input = program.at<ArgumentParser>(command).get("-i");
        if (input == "-")
        {
            /// TODO: for now we do only support yaml files over the cin.
            /// We might want to either remove it cin support or add checking logic (yaml vs test)
            decomposedQueryPlan = NES::CLI::loadFrom(std::cin);
        }
        else
        {
            if (input.ends_with(".yaml"))
            {
                decomposedQueryPlan = NES::CLI::loadFromYAMLFile(input);
            }
            else if (input.ends_with(".test") || input.ends_with(".test_nightly"))
            {
                /// Get from input the filename without the extension
                auto testname = std::filesystem::path(input).stem().string();
                auto outputDir = program.at<ArgumentParser>(command).get("-o");
                /// Check if it is a directory and not a file
                if (!std::filesystem::is_directory(outputDir))
                {
                    NES_FATAL_ERROR("The output is not a directory: {}", outputDir);
                    std::exit(1);
                }

                /// A SqlLogicTest format file might have >=1 tests
                auto decomposedQueryPlans = NES::CLI::loadFromSLTFile(input, testname);
                for (std::size_t testnr = 0; const auto& plan : decomposedQueryPlans)
                {
                    SerializableDecomposedQueryPlan serialized;
                    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(plan, &serialized);
                    auto outfilename = std::string(outputDir + "/" + testname + "_" + std::to_string(testnr) + ".pb");
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
                        return -1;
                    }
                    std::string output;
                    google::protobuf::TextFormat::PrintToString(serialized, &output);
                    NES_INFO("GRPC QueryPlan: {}", output);
                    ++testnr;
                }
            }
            exit(0);
        }
    }
    catch (...)
    {
        tryLogCurrentException();
        exit(1);
    }

    std::string output;
    SerializableDecomposedQueryPlan serialized;
    DecomposedQueryPlanSerializationUtil::serializeDecomposedQueryPlan(decomposedQueryPlan, &serialized);
    google::protobuf::TextFormat::PrintToString(serialized, &output);
    NES_INFO("GRPC QueryPlan: {}", output);
    if (program.is_subcommand_used("dump"))
    {
        auto& dumpArgs = program.at<ArgumentParser>("dump");
        auto outputPath = dumpArgs.get<std::string>("-o");
        std::ostream* output;
        std::ofstream file;
        if (outputPath == "-")
        {
            output = &std::cout;
        }
        else
        {
            file = std::ofstream(outputPath);
            if (!file)
            {
                NES_FATAL_ERROR("Could not open output file: {}", outputPath);
                std::exit(1);
            }
            output = &file;
        }
        if (!serialized.SerializeToOstream(output))
        {
            NES_FATAL_ERROR("Failed to write message to file.");
            return -1;
        }

        if (outputPath == "-")
        {
            NES_INFO("Wrote protobuf to {}", outputPath);
        }
    }
    else if (program.is_subcommand_used("register"))
    {
        auto& registerArgs = program.at<ArgumentParser>("register");
        GRPCClient client(grpc::CreateChannel(registerArgs.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
        auto queryId = client.registerQuery(decomposedQueryPlan);
        if (registerArgs.is_used("-x"))
        {
            client.start(queryId);
        }
        std::cout << queryId;
    }

    return 0;
}
