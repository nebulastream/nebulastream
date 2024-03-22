//
// Created by ls on 28.09.23.
//

#ifndef NES_OPTIONS_H
#define NES_OPTIONS_H

#include "YAMLModel.h"
#include <API/Schema.hpp>
#include <Identifiers.hpp>
#include <argumentum/argparse.h>
#include <boost/filesystem.hpp>
#include <boost/outcome.hpp>
#include <string>

struct Options {
    NES::NodeId nodeId;
    NES::QueryId queryId;
    NES::QuerySubPlanId subQueryId;
    NES::OperatorId operatorId;
    size_t workerId;
    std::string hostIp = "127.0.0.1";
    uint32_t port = 8082;
    std::vector<std::pair<WorkerConfiguration, WorkerLinkConfiguration>> upstreams;
    NES::SchemaPtr outputSchema;
    bool print;
    std::optional<uint64_t> latency;

    using Result = boost::outcome_v2::result<Options, std::string>;
    static Result fromCLI(int argc, char** argv);

    static std::vector<std::pair<WorkerConfiguration, WorkerLinkConfiguration>>
    findUpstreamWorker(const SinkEndpointConfiguration& configuration, const std::vector<WorkerConfiguration>& workers);
};

#endif//NES_OPTIONS_H
