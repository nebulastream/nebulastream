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

#include <YAMLModel.h>
#include <argumentum/argparse.h>
#include <yaml-cpp/yaml.h>
#define BOOST_PROCESS_NO_DEPRECATED 1
#include <Util/Logger/Logger.hpp>
#include <boost/asio.hpp>
#include <boost/outcome.hpp>
#include <boost/process.hpp>
#include <cstddef>
#include <ranges>
#include <tuple>
#include <vector>

namespace stdr = std::ranges;
namespace stdv = std::ranges::views;

std::tuple<std::vector<size_t>, std::vector<size_t>> getStagesAndHandler(const WorkerConfiguration& worker) {
    std::vector<size_t> stages;
    std::vector<size_t> sharedHandler;
    for (const auto& subQuery : worker.subQueries) {
        sharedHandler.emplace_back(subQuery.subQueryId);
        std::vector<const WorkerStageConfiguration*> stack;
        stack.emplace_back(&subQuery.stages);
        while (!stack.empty()) {
            auto current = stack.back();
            stack.pop_back();

            stages.emplace_back(current->stageId);
            if (current->predecessor.has_value()) {
                for (const auto& c : *current->predecessor) {
                    stack.emplace_back(&c);
                }
            }
        }
    }

    return {stages, sharedHandler};
}
#define OPERATOR_DIR "/nes-unikernel/operators"
#define SRC_DIR "/nes-unikernel/src"

namespace bp = boost::process;
void build_unikernel(const WorkerConfiguration& worker,
                     const std::vector<std::string>& additionalFlags,
                     const std::string& nesSourceDir,
                     const std::string& nesDependencyDir) {
    auto [stages, sharedHandler] = getStagesAndHandler(worker);

    auto fileExists = stdv::filter([](const auto& path) {
        return boost::filesystem::exists(path);
    });
    auto toOperatorPath = [&nesSourceDir](const auto& prefix) {
        return stdv::transform([&prefix, &nesSourceDir](const auto& a) {
            return fmt::format("{}/{}{}.o", nesSourceDir + OPERATOR_DIR, prefix, a);
        });
    };
    auto prefix = [](const auto& s) {
        return stdv::transform([s](const auto& a) {
            return s + a;
        });
    };

    std::vector<std::string> includePaths{"/nes-unikernel/include/",
                                          "/nes-runtime/include/",
                                          "/nes-common/include/",
                                          "/nes-operators/include/",
                                          "/nes-core/include/",
                                          "/nes-data-types/include/",
                                          "/nes-configurations/include/"};
    std::vector<std::string> defines{"NES_COMPILE_TIME_LOG_LEVEL=0", "UNIKERNEL_LIB"};
    std::vector<std::string> libs{"zmq-fork", "spdlog", "fmt", "dw", "folly", "glog", "gflags"};
    std::vector<std::string> cxxFlags{"--std=c++20"};

    std::vector<std::string> args;
    args.push_back(fmt::format("{}/main.cpp", nesSourceDir + SRC_DIR));
    stdr::copy(stages | toOperatorPath("stage"), std::back_inserter(args));
    stdr::copy(sharedHandler | toOperatorPath("sharedHandlers") | fileExists, std::back_inserter(args));
    args.emplace_back("/home/ls/DIMA/nebulastream/cmake-build-unikernelexport-clang-16-debug/nes-unikernel/libunikernel-lib.a");
    stdr::copy(cxxFlags, std::back_inserter(args));
    stdr::copy(includePaths | prefix("-I" + nesSourceDir), std::back_inserter(args));
    args.emplace_back("-I" + nesDependencyDir + "/include/");
    stdr::copy(defines | prefix("-D"), std::back_inserter(args));
    stdr::copy(libs | prefix("-l"), std::back_inserter(args));
    args.emplace_back("-L" + nesDependencyDir + "/lib/");
    stdr::copy(additionalFlags, std::back_inserter(args));
    NES_DEBUG("Calling Clang with Args: {}", fmt::join(args, " \\\n"));

    bp::ipstream err_pipe;
    std::error_code ec;
    auto c = bp::child("/usr/bin/clang++", bp::args(args), bp::std_err > err_pipe, ec);
    c.wait();

    if (ec) {
        std::stringstream ss;
        std::string line;
        while (err_pipe && std::getline(err_pipe, line)) {
            ss << line << std::endl;
        }
        NES_ERROR("Unikernel Build Failed:\n{}", ss.str());
    } else {
        NES_INFO("Unikernel {} was built sucessfully", worker.nodeId);
    }
}

void launchSource(bp::group& group, const std::string& configPath, size_t sourceId, const std::string& binaryFile) {

    std::vector<std::string> args{"-c", configPath, std::to_string(sourceId)};
    bp::spawn(binaryFile, bp::args(args), group);
}

int main(int argc, char** argv) {
    NES::Logger::setupLogging(NES::LogLevel::LOG_DEBUG);
    std::string configFilePath;
    std::string outputPath = "-";
    std::string sinkBinary = "unikernel-sink";
    std::string sourceBinary = "unikernel-source";
    std::string nesSourceDirectory = ".";
    std::string nesDependencies = ".";
    auto parser = argumentum::argument_parser{};
    auto params = parser.params();
    params.add_parameter(configFilePath, "-c", "--configPath").maxargs(1).help("Path to Config YAML");
    params.add_parameter(sinkBinary, "-x", "--sinkPath").maxargs(1).help("Path to Unikernel Sink");
    params.add_parameter(sourceBinary, "-s", "--sourcePath").maxargs(1).help("Path to Unikernel Source");
    params.add_parameter(nesSourceDirectory, "-n", "--nesSource").maxargs(1).help("Help to the NebulaStream Directory");
    params.add_parameter(nesDependencies, "-d", "--dependency").maxargs(1).help("Path to the NES dependencies ../x64-linux-nes");
    params.add_parameter(outputPath, "-o").maxargs(1).help("Path to output the generated Header File");
    parser.parse_args(argc, argv);

    std::vector<std::string> additionalArgs;

    auto config = YAML::LoadFile(configFilePath).as<Configuration>();
    for (const auto& item : config.workers) {
        build_unikernel(item, {"-v", "-fsanitize=thread"}, nesSourceDirectory, nesDependencies);
    }

//    boost::asio::io_context context;
//    bp::group sources;
//    int i = 0;
//    for (const auto& source : config.sources) {
//        std::cout << "Launching Source: " << i << std::endl;
//        launchSource(sources, configFilePath, i, sourceBinary);
//        sleep(3);
//        i++;
//    }
//
//    sources.wait();
}
