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
#include <boost/asio.hpp>
#include <boost/outcome.hpp>
#include <boost/process.hpp>

namespace bp = boost::process;
void launchSource(bp::group& group, const std::string& configPath, size_t sourceId, const std::string& binaryFile) {

    std::vector<std::string> args{"-c", configPath, std::to_string(sourceId)};
    bp::spawn(binaryFile, bp::args(args), group);
}

int main(int argc, char** argv) {
    std::string configFilePath;
    std::string outputPath = "-";
    std::string sinkBinary = "unikernel-sink";
    std::string sourceBinary = "unikernel-source";
    auto parser = argumentum::argument_parser{};
    auto params = parser.params();
    params.add_parameter(configFilePath, "-c", "--configPath").maxargs(1).help("Path to Config YAML");
    params.add_parameter(sinkBinary, "-x", "--sinkPath").maxargs(1).help("Path to Unikernel Sink");
    params.add_parameter(sourceBinary, "-s", "--sourcePath").maxargs(1).help("Path to Unikernel Source");
    params.add_parameter(outputPath, "-o").maxargs(1).help("Path to output the generated Header File");
    parser.parse_args(argc, argv);

    auto config = YAML::LoadFile(configFilePath).as<Configuration>();

    boost::asio::io_context context;
    bp::group sources;
    int i = 0;
    for (const auto& source : config.sources) {
        std::cout << "Launching Source: " << i << std::endl;
        launchSource(sources, configFilePath, i, sourceBinary);
        sleep(3);
        i++;
    }

    sources.wait();
}
