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

#include <API/Schema.hpp>
#include <CLIOptions.h>
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Catalogs/Topology/Topology.hpp>
#include <Catalogs/Topology/TopologyNode.hpp>
#include <Configurations/Worker/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Configurations/WorkerPropertyKeys.hpp>
#include <NoOpPhysicalSource.h>
#include <Util/magicenum/magic_enum.hpp>
#include <argumentum/argparse.h>
#include <sys/stat.h>
#include <yaml-cpp/yaml.h>

YAML::Node& Options::loadInputYaml() {
    if (yamlRoot.has_value()) {
        return yamlRoot.value();
    }
    YAML::Node node;
    std::string fileContent;
    if (input) {
        std::ifstream f(input->c_str());
        node = YAML::LoadFile(input->c_str());
    } else {
        node = YAML::Load(std::cin);
    }

    this->yamlRoot.emplace(std::move(node));
    return yamlRoot.value();
}
std::string Options::getQueryString() {
    auto& yaml = loadInputYaml();
    return yaml["query"].as<std::string>();
}

NES::SchemaPtr Options::parseSchema(YAML::Node schemaNode) {
    auto schema = NES::Schema::create();
    for (const auto& field : schemaNode) {
        assert(field.IsMap());
        assert(!field["name"].IsNull());
        assert(!field["type"].IsNull());
        auto type = magic_enum::enum_cast<NES::BasicType>(field["type"].as<std::string>());
        schema = schema->addField(field["name"].as<std::string>(), type.value());
    }

    return schema;
}

size_t toNodeId(std::string nodeIdString) {
    if (nodeIdString.empty())
        NES_THROW_RUNTIME_ERROR("Failed to parse NodeID: Empty String");

    bool isNumeric = std::find_if(nodeIdString.begin(),
                                  nodeIdString.end(),
                                  [](unsigned char c) {
                                      return !std::isdigit(c);
                                  })
        == nodeIdString.end();
    if (isNumeric)
        return std::stol(nodeIdString);

    std::transform(nodeIdString.begin(), nodeIdString.end(), nodeIdString.begin(), [](unsigned char c) {
        return std::tolower(c);
    });

    if (nodeIdString == "sink") {
        return 1;
    }

    NES_THROW_RUNTIME_ERROR("Failed to parse NodeID: " + nodeIdString);
}

std::tuple<NES::TopologyPtr, NES::Catalogs::Source::SourceCatalogPtr, std::vector<NES::PhysicalSourceTypePtr>>
Options::getTopologyAndSources() {
    auto sourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();
    std::vector<NES::PhysicalSourceTypePtr> physicalSources;
    auto topology = NES::Topology::create();
    auto& root = loadInputYaml();
    YAML::Node yamlTopology = root["topology"];

    std::unordered_map<size_t, NES::TopologyNodePtr> nodes;

    nodes[1] = NES::TopologyNode::create(1,
                                         yamlTopology["sink"]["ip"].as<std::string>(),
                                         yamlTopology["sink"]["port"].as<uint16_t>(),
                                         yamlTopology["sink"]["port"].as<uint16_t>(),
                                         yamlTopology["sink"]["resources"].as<uint16_t>(),
                                         {{NES::Worker::Properties::MAINTENANCE, false}});
    topology->setAsRoot(nodes[1]);

    assert(yamlTopology["workers"].IsSequence());
    for (size_t i = 0; i < yamlTopology["workers"].size(); ++i) {
        YAML::Node workerYaml = yamlTopology["workers"][i];
        auto worker = NES::TopologyNode::create(i + 2,
                                                workerYaml["ip"].as<std::string>(),
                                                workerYaml["port"].as<uint16_t>(),
                                                workerYaml["port"].as<uint16_t>(),
                                                workerYaml["resources"].as<uint16_t>(),
                                                {{NES::Worker::Properties::MAINTENANCE, false}});
        for (const auto& source : workerYaml["sources"]) {
            auto sourceName = source["name"].as<std::string>();
            auto schema = parseSchema(source["schema"]);
            sourceCatalog->addLogicalSource(sourceName, schema);
            auto logicalSource = sourceCatalog->getLogicalSource(sourceName);
            auto physicalSourceType = std::make_shared<NES::NoOpPhysicalSourceType>(sourceName, sourceName);
            auto physicalSource = NES::PhysicalSource::create(physicalSourceType);
            sourceCatalog->addPhysicalSource(
                sourceName,
                NES::Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, worker));
            physicalSources.push_back(physicalSourceType);
        }

        nodes[i + 2] = std::move(worker);
    }

    for (size_t i = 0; i < yamlTopology["workers"].size(); ++i) {
        YAML::Node workerYaml = yamlTopology["workers"][i];
        assert(workerYaml["links"].IsSequence());
        for (size_t linkIdx = 0; linkIdx < workerYaml["links"].size(); linkIdx++) {
            auto otherNodeId = toNodeId(workerYaml["links"][linkIdx].as<std::string>());
            auto parent = std::min(otherNodeId, i + 2);
            auto child = std::max(otherNodeId, i + 2);
            topology->addNewTopologyNodeAsChild(nodes[parent], nodes[child]);
            nodes[parent]->addLinkProperty(nodes[child], std::make_shared<LinkProperty>(100, 100));
        }
    }

    return {topology, sourceCatalog, physicalSources};
}

std::string Options::getOutputPathForFile(const std::string& file) {
    if (output) {
        return output.value() + "/" + file;
    } else {
        return file;
    }
}

static bool isDirectoryExists(const std::string& path) {
    struct stat info;

    if (stat(path.c_str(), &info) != 0) {
        // If stat fails, the file/directory does not exist.
        return false;
    }

    return (info.st_mode & S_IFDIR) != 0;
}

static bool exists(const std::string& name) {
    std::ifstream f(name.c_str());
    return f.good();
}

CLIResult Options::getCLIOptions(int argc, char** argv) {
    using namespace argumentum;
    auto parser = argument_parser{};
    auto params = parser.params();
    parser.config().program(argv[0]).description("Unikernel Export");
    std::string input;
    std::string output;
    std::string yamlOutput;
    int bufferSize = 8192;
    params.add_parameter(input, "input").nargs(1).metavar("INPUT PATH").help("Input path (use - for stdin)");
    params.add_parameter(bufferSize, "--bufferSize", "-b").nargs(1).metavar("BUFFER SIZE").help("Buffer size");
    params.add_parameter(output, "--output", "-o").nargs(1).metavar("OUTPUT PATH").help("Output path");
    params.add_parameter(yamlOutput, "--yaml", "-y")
        .nargs(1)
        .metavar("YAML OUTPUT PATH")
        .help("Output path for generated yaml file");
    if (!parser.parse_args(argc, argv, 1))
        exit(1);

    Options options;
    if (input == "-") {
        options.input = std::nullopt;
    } else {
        if (!exists(input))
            return "Input file does not exist";
        options.input = input;
    }

    options.bufferSize = bufferSize;

    if (output.empty()) {
        options.output = std::nullopt;
    } else {
        if (!isDirectoryExists(output))
            return "Output directory does not exist";
        options.output = output;
    }

    if (yamlOutput.empty()) {
        options.yamlOutput = "./export.yaml";
    } else {
        options.yamlOutput = yamlOutput;
    }

    return options;
}
size_t Options::getBufferSize() const { return bufferSize; }
std::string Options::getYAMLOutputPath() const { return this->yamlOutput; }
