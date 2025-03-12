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

#include <csignal>
#include <fstream>

#include <Configurations/Util.hpp>
#include <Operators/Serialization/DecomposedQueryPlanSerializationUtil.hpp>
#include <Util/Logger/Logger.hpp>
#include <argparse/argparse.hpp>
#include <fmt/format.h>
#include <google/protobuf/text_format.h>
#include <grpcpp/server_builder.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SerializableDecomposedQueryPlan.pb.h>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

extern void init_receiver_server_string(const std::string& connection);
extern void init_sender_server();

int main(const int argc, const char* argv[])
{
    try
    {
        NES::Logger::setupLogging("worker.log", NES::LogLevel::LOG_DEBUG);
        argparse::ArgumentParser program("worker");
        program.add_argument("port").help("data port").scan<'i', int>();
        program.add_argument("queryplan").help("decomposed queryplan to compile and run");

        program.parse_args(argc, argv);

        auto port = program.get<int>("port");
        init_receiver_server_string(fmt::format("127.0.0.1:{}", port));
        init_sender_server();

        const auto planFile = program.get<std::string>("queryplan");
        std::ifstream ifs{planFile};

        NES::SerializableDecomposedQueryPlan lan;
        if (planFile.ends_with(".txtpb"))
        {
            std::stringstream ss;
            ss << ifs.rdbuf();
            google::protobuf::TextFormat::ParseFromString(ss.str(), &lan);
        }
        else if (planFile.ends_with(".binpb"))
        {
            if (!lan.ParseFromIstream(&ifs))
            {
                NES_ERROR("Reading SerialirableDecomposedQueryPlan from file {} failed", planFile);
            }
        }
        else
        {
            std::cout << "pls give file ending with .txtpb or .binpb" << std::endl;
        }

        auto plan = NES::DecomposedQueryPlanSerializationUtil::deserializeDecomposedQueryPlan(&lan);

        NES::Configuration::SingleNodeWorkerConfiguration conf;
        NES::SingleNodeWorker worker{conf};

        auto qid = worker.registerQuery(plan);

        worker.startQuery(qid);

        for (;;)
        {
        };
    }
    catch (const NES::Exception& e)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentExceptionCode();
    }
    catch (const std::exception& err)
    {
        std::cerr << err.what() << std::endl;
        std::exit(1);
    }
    catch (...)
    {
        return NES::ErrorCode::UnknownException;
    }
}
