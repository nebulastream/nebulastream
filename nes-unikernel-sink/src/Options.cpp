//
// Created by ls on 09.10.23.
//

#include "Util/Logger/Logger.hpp"
#include <Options.h>
#include <Util/magicenum/magic_enum.hpp>
#include <YAMLModel.h>
#include <yaml-cpp/yaml.h>

boost::outcome_v2::result<NES::SchemaPtr, std::string> parseSchema(const SchemaConfiguration& schemaConfig) {
    NES::SchemaPtr schema = NES::Schema::create();
    for (const auto& field : schemaConfig.fields) {
        schema = schema->addField(field.name, magic_enum::enum_cast<NES::BasicType>(field.type).value());
    }
    return schema;
}

Options::Result Options::fromCLI(int argc, char** argv) {
    using namespace argumentum;
    auto parser = argument_parser{};
    auto params = parser.params();
    std::optional<size_t> sinkId;
    std::string filepath = "./testConfig.yaml";
    parser.config().program(argv[0]).description("Unikernel Source");
    params.add_parameter(filepath, "-c", "--config").nargs(1).metavar("PATH").help("path to config.yaml");
    params.add_parameter(sinkId, "-n", "--sink-id")
        .nargs(1)
        .metavar("SINK_ID")
        .help("Id of the source to use (currently index of the sink in the list of sinks)");

    bool print = false;
    params.add_parameter(print, "-p").nargs(0);
    auto result = parser.parse_args(argc, argv);
    if (result.errors_were_shown()) {
        return "Arg parsing";
    }

    if (!boost::filesystem::exists(filepath)) {
        return "\'" + filepath + "\' File does not Exists";
    }

    auto configuration = YAML::LoadFile(filepath).as<Configuration>();
    auto sink = configuration.sink;

    auto schemaResult = parseSchema(sink.schema);
    if (schemaResult.has_error())
        return schemaResult.as_failure();
    auto schema = schemaResult.value();
    auto upstreams = findUpstreamWorker(sink, configuration.workers);

    return Options{sink.nodeId,
                   configuration.query.queryID,
                   sink.subQueryID,
                   sink.operatorId,
                   configuration.query.workerID,
                   sink.ip,
                   sink.port,
                   upstreams,
                   schema,
                   print};
}

std::vector<std::pair<WorkerConfiguration, WorkerLinkConfiguration>>
Options::findUpstreamWorker(const SinkEndpointConfiguration& configuration, const std::vector<WorkerConfiguration>& workers) {
    std::vector<std::pair<WorkerConfiguration, WorkerLinkConfiguration>> pairs;
    for (const auto& worker : workers) {
        for (const auto& sq : worker.subQueries) {
            NES_ASSERT(sq.type == WorkerDownStreamLinkConfigurationType::node && sq.worker.has_value(),
                       "Unikernel Sink is only needed if kafka is not used");
            if (sq.worker->ip == configuration.ip && sq.worker->port == configuration.port) {
                pairs.emplace_back(worker, *sq.worker);
            }
        }
    }
    return pairs;
}
