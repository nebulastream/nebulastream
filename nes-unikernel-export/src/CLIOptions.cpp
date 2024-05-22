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
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <NoOp/NoOpPhysicalSourceType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <argumentum/argparse.h>
#include <filesystem>
#include <sys/stat.h>
#include <variant>
#include <yaml-cpp/yaml.h>

std::string Options::getQueryString() { return configuration.query; }

bool Options::useKafka() const { return configuration.topology.sink.kafka.has_value(); }
ExportKafkaConfiguration Options::getKafkaConfiguration() const {
    NES_ASSERT(configuration.topology.sink.kafka.has_value(), "Kafka is not used");
    return configuration.topology.sink.kafka.value();
}

NES::SchemaPtr getBenchmarkSchema(SchemaType type) {
    switch (type) {
        case NEXMARK_BID: return std::make_unique<NES::Benchmark::DataGeneration::NEBitDataGenerator>()->getSchema();
        case NEXMARK_PERSON: NES_NOT_IMPLEMENTED();
        case NEXMARK_AUCTION:
            return std::make_unique<NES::Benchmark::DataGeneration::NEAuctionDataGenerator>()->getSchema();
            break;
        default: NES_NOT_IMPLEMENTED();
    }
}

NES::SchemaPtr parseSchema(const SchemaConfiguration& schemaConfig) {
    if (schemaConfig.type != MANUAL) {
        return getBenchmarkSchema(schemaConfig.type);
    }
    auto schema = NES::Schema::create();
    for (const auto& field : schemaConfig.fields) {
        auto type = magic_enum::enum_cast<NES::BasicType>(field.type);
        schema = schema->addField(field.name, type.value());
    }

    return schema;
}

void Options::createSource(std::shared_ptr<NES::Catalogs::Source::SourceCatalog> sourceCatalog,
                           std::vector<NES::PhysicalSourceTypePtr> physicalSources,
                           NES::WorkerId workerId,
                           const ExportSourceConfiguration& source) {

    if (!sourceCatalog->containsLogicalSource(source.name)) {
        sourceCatalog->addLogicalSource(source.name, parseSchema(source.schema));
    }

    auto logicalSource = sourceCatalog->getLogicalSource(source.name);
    auto physicalSourceName = fmt::format("{}_phys_{}", source.name, workerId);
    auto physicalSourceType =
        std::make_shared<NES::NoOpPhysicalSourceType>(source.name, physicalSourceName, source.schema.type, source.tcp);
    auto physicalSource = NES::PhysicalSource::create(physicalSourceType);
    sourceCatalog->addPhysicalSource(source.name,
                                     NES::Catalogs::Source::SourceCatalogEntry::create(physicalSource, logicalSource, workerId));
    physicalSources.push_back(physicalSourceType);
}
std::tuple<NES::TopologyPtr, NES::Catalogs::Source::SourceCatalogPtr, std::vector<NES::PhysicalSourceTypePtr>>
Options::getTopologyAndSources() {
    auto sourceCatalog = std::make_shared<NES::Catalogs::Source::SourceCatalog>();
    std::vector<NES::PhysicalSourceTypePtr> physicalSources;
    auto topology = NES::Topology::create();
    if (useKafka()) {
        topology->registerWorkerAsRoot(NES::WorkerId(1),
                                       "0.0.0.0",
                                       0,
                                       0,
                                       1,
                                       {{NES::Worker::Properties::MAINTENANCE, false}},
                                       100,
                                       30);
    } else {
        const auto& sink = configuration.topology.sink.node.value();
        topology->registerWorkerAsRoot(NES::WorkerId(1),
                                       sink.ip,
                                       sink.port,
                                       sink.port,
                                       sink.resources,
                                       {{NES::Worker::Properties::MAINTENANCE, false}},
                                       100,
                                       30);
    }

    for (size_t i = 0; const auto& workerConfiguration : configuration.topology.workers) {
        topology->registerWorker(NES::WorkerId(i + 2),
                                 workerConfiguration.node.ip,
                                 workerConfiguration.node.port,
                                 workerConfiguration.node.port,
                                 workerConfiguration.node.resources,
                                 {{NES::Worker::Properties::MAINTENANCE, false}},
                                 100,
                                 30);

        for (const auto& source : workerConfiguration.sources) {
            createSource(sourceCatalog, physicalSources, NES::WorkerId(i + 2), source);
        }

        i++;
    }

    for (size_t i = 0; const auto& workerConfiguration : configuration.topology.workers) {
        for (const auto& otherNode : workerConfiguration.links) {
            auto otherNodeId = getOtherNodeIdFromLink(otherNode);
            auto parent = std::min(otherNodeId, i + 2);
            auto child = std::max(otherNodeId, i + 2);
            topology->addTopologyNodeAsChild(NES::WorkerId(parent), NES::WorkerId(child));
        }
        i++;
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
    struct stat info {};

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
    YAML::Node yamlConfig;
    try {
        if (input == "-") {
            yamlConfig = YAML::Load(std::cin);
        } else {
            if (!exists(input))
                return "Input file does not exist";
            yamlConfig = YAML::LoadFile(input.c_str());
        }
    } catch (const YAML::ParserException& pe) {
        return "Could not parse YAML File: " + pe.msg;
    } catch (const YAML::BadFile& be) {
        return "Could not open YAML File: " + be.msg;
    }
    try {
        options.configuration = yamlConfig.as<ExportConfiguration>();
    } catch (const YAML::BadConversion& bc) {
        return "Could not parse ExportConfiguration: " + bc.msg;
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

std::filesystem::path Options::getStageOutputPathForNode(NES::WorkerId nodeId) const {
    namespace fs = std::filesystem;

    auto path = fs::path(output.value_or(fs::current_path().string()));

    NES_ASSERT2_FMT(is_directory(path), fmt::format("Path does not exist: {}", path.string()));

    auto nodePath = path / fmt::format("node{}", nodeId);
    fs::create_directories(nodePath);

    return nodePath;
}

size_t Options::getOtherNodeIdFromLink(const std::variant<std::string, size_t>& variant) {
    if (std::holds_alternative<std::string>(variant)) {
        assert(std::get<std::string>(variant) == "sink" && "'sink' is the only accepted string parameter for links");
        return 1;
    }
    return std::get<size_t>(variant);
}
