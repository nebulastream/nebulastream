//
// Created by ls on 09.10.23.
//

#include "Util/Logger/Logger.hpp"
#include <API/Schema.hpp>
#include <DataGeneration/Nextmark/NEAuctionDataGenerator.hpp>
#include <DataGeneration/Nextmark/NEBitDataGenerator.hpp>
#include <Options.h>
#include <Util/magicenum/magic_enum.hpp>
#include <YAMLModel.h>

boost::outcome_v2::result<NES::SchemaPtr, std::string> parseSchema(const SchemaConfiguration& schemaConfig) {
    NES::SchemaPtr schema = NES::Schema::create();
    for (const auto& field : schemaConfig.fields) {
        schema = schema->addField(field.name, magic_enum::enum_cast<NES::BasicType>(field.type).value());
    }
    return schema;
}

static std::pair<WorkerConfiguration, WorkerLinkConfiguration>
findDownstreamWorker(const SourceEndpointConfiguration& configuration, const std::vector<WorkerConfiguration>& workers) {
    for (const auto& worker : workers) {
        for (const auto& sq : worker.subQueries) {

            if (sq.upstream.has_value() && sq.upstream->worker.has_value()) {
                auto upstream = *sq.upstream->worker;
                if (upstream.ip == configuration.ip && upstream.port == configuration.port) {
                    return {worker, upstream};
                }
                continue;
            } else if (sq.upstream.has_value()) {
                continue;
            }

            std::queue<const WorkerStageConfiguration*> q;
            q.push(&sq.stage.value());
            while (!q.empty()) {
                auto current = q.front();
                q.pop();
                if (current->upstream.has_value() && current->upstream->worker.has_value()) {
                    auto upstream = *current->upstream->worker;
                    if (upstream.ip == configuration.ip && upstream.port == configuration.port) {
                        return {worker, upstream};
                    }
                }
                if (current->predecessor.has_value()) {
                    for (const auto& pred : current->predecessor.value()) {
                        q.push(&pred);
                    }
                }
            }
        }
    }
    NES_THROW_RUNTIME_ERROR("Could not Find Source");
}

std::optional<long> getSourceIdFromEnvironment() {
    auto envValue = std::getenv("SOURCE_ID");
    if (!envValue) {
        return {};
    }
    errno = 0;
    auto parsed_value = std::strtol(envValue, nullptr, 10);
    if (errno != 0) {
        return {};
    }

    return parsed_value;
}

Options::Result Options::fromCLI(int argc, char** argv) {
    using namespace argumentum;
    auto parser = argument_parser{};
    auto params = parser.params();

    std::optional<long> sourceIdOpt;
    std::string filepath = "./testConfig.yaml";
    std::string dataSourceType = std::string("ADHOC_GENERATOR");
    std::string path;
    bool print = false;
    size_t numberOfTupleBuffers = 32;
    size_t delayInMillies = 0;

    parser.config().program(argv[0]).description("Unikernel Source");
    params.add_parameter(filepath, "-c", "--config").nargs(1).metavar("PATH").help("path to config.yaml");
    params.add_parameter(sourceIdOpt, "-n", "--source-id")
        .nargs(1)
        .metavar("SOURCE_ID")
        .help("Id of the source to use (currently index of the source in the list of sources)");

    params.add_parameter(dataSourceType, "-t", "--type")
        .nargs(1)
        .required(false)
        .metavar("DATA_SOURCE_TYPE")
        .help("DATA_FILE or ADHOC_GENERATOR (default)");

    params.add_parameter(path, "-f", "--file").nargs(1);

    params.add_parameter(print, "-p", "--print").nargs(0).required(false);
    params.add_parameter(numberOfTupleBuffers, "-s", "--size").nargs(1).required(false);
    params.add_parameter(delayInMillies, "-d", "--delay").nargs(1).required(false);

    parser.parse_args(argc, argv);

    if (!boost::filesystem::exists(filepath)) {
        return "\'" + filepath + "\' File does not Exists";
    }

    auto configuration = YAML::LoadFile(filepath).as<Configuration>();
    auto sources = configuration.sources;
    auto sourceId = sourceIdOpt.value_or(getSourceIdFromEnvironment().value_or(-1));
    NES_ASSERT2_FMT(sourceId >= 0, "Missing SourceID");
    NES_ASSERT2_FMT(static_cast<size_t>(sourceId) < sources.size(), "Source, does not exist");
    auto source = sources[sourceId];

    auto schemaResult = parseSchema(source.schema);
    if (schemaResult.has_error())
        return schemaResult.as_failure();
    auto schema = schemaResult.value();

    if (source.type == NetworkSource) {

        auto [worker, downstream] = findDownstreamWorker(source, configuration.workers);

        return Options{source.nodeId,
                       configuration.query.queryID,
                       source.subQueryID,
                       downstream.operatorId,
                       configuration.query.workerID,
                       source.ip,
                       source.port,
                       worker.ip,
                       worker.port,
                       worker.nodeId,
                       source.originId,
                       downstream.partitionId,
                       downstream.subpartitionId,
                       schema,
                       delayInMillies,
                       8192,
                       source.format.value_or(NES::FormatTypes::CSV_FORMAT),
                       source.schema.type,
                       source.type,
                       magic_enum::enum_cast<DataSourceType>(dataSourceType).value_or(ADHOC_GENERATOR),
                       path,
                       numberOfTupleBuffers,
                       print};
    } else if (source.type == TcpSource) {
        return Options{0,
                       0,
                       0,
                       0,
                       0,
                       source.ip,
                       source.port,
                       "",
                       0,
                       0,
                       0,
                       0,
                       0,
                       schema,
                       delayInMillies,
                       8192,
                       source.format.value_or(NES::FormatTypes::CSV_FORMAT),
                       source.schema.type,
                       source.type,
                       magic_enum::enum_cast<DataSourceType>(dataSourceType).value_or(ADHOC_GENERATOR),
                       path,
                       numberOfTupleBuffers,
                       print};
    } else {
        NES_NOT_IMPLEMENTED();
    }
}
NES::SchemaPtr Options::getSchema() const {
    switch (this->schemaType) {
        case NEXMARK_BID: return std::make_unique<NES::Benchmark::DataGeneration::NEBitDataGenerator>()->getSchema();
        case NEXMARK_PERSON: NES_NOT_IMPLEMENTED();
        case NEXMARK_AUCTION: return std::make_unique<NES::Benchmark::DataGeneration::NEAuctionDataGenerator>()->getSchema();
        case MANUAL: return schema;
        default: NES_NOT_IMPLEMENTED();
    }
}