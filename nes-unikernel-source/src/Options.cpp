//
// Created by ls on 09.10.23.
//

#include "Util/Logger/Logger.hpp"
#include <API/Schema.hpp>
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
findDownstreamWorker(const EndpointConfiguration& configuration, const std::vector<WorkerConfiguration>& workers) {
    for (const auto& worker : workers) {
        for (const auto& sq : worker.subQueries) {
            std::queue<const WorkerStageConfiguration*> q;
            q.push(&sq.stages);
            while (!q.empty()) {
                auto current = q.front();
                q.pop();
                if (current->upstream.has_value()) {
                    auto upstream = *current->upstream;
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
Options::Result Options::fromCLI(int argc, char** argv) {
    using namespace argumentum;
    auto parser = argument_parser{};
    auto params = parser.params();

    size_t sourceId;

    std::string filepath = "./testConfig.yaml";

    parser.config().program(argv[0]).description("Unikernel Source");
    params.add_parameter(filepath, "-c", "--config").nargs(1).metavar("PATH").help("path to config.yaml");
    params.add_parameter(sourceId, "N", "SOURCE_ID")
        .nargs(1)
        .metavar("SOURCE_ID")
        .help("Id of the source to use (currently index of the source in the list of sources)");

    parser.parse_args(argc, argv);
    if (!boost::filesystem::exists(filepath)) {
        return "\'" + filepath + "\' File does not Exists";
    }

    auto configuration = YAML::LoadFile(filepath).as<Configuration>();
    auto sources = configuration.sources;
    NES_ASSERT2_FMT(sourceId < sources.size(), "Source, does not exist");
    auto source = sources[sourceId];

    auto schemaResult = parseSchema(source.schema);
    if (schemaResult.has_error())
        return schemaResult.as_failure();
    auto schema = schemaResult.value();
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
                   schema};
}