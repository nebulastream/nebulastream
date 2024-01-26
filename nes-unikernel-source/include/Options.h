//
// Created by ls on 28.09.23.
//

#ifndef NES_OPTIONS_H
#define NES_OPTIONS_H

#include <Identifiers.hpp>
#include <Sinks/Formats/FormatType.hpp>
#include <YAMLModel.h>
#include <argumentum/argparse.h>
#include <boost/filesystem.hpp>
#include <boost/outcome.hpp>

struct Options {
    NES::NodeId nodeId;
    NES::QueryId queryId;
    NES::QuerySubPlanId subQueryId;
    NES::OperatorId operatorId;
    size_t workerId;
    std::string hostIp = "127.0.0.1";
    uint32_t port = 8082;
    std::string downstreamIp = "127.0.0.1";
    uint32_t downstreamPort = 8080;
    NES::NodeId downstreamId;
    NES::OriginId originId;
    NES::PartitionId partitionId;
    NES::SubpartitionId subPartitionId;
    NES::SchemaPtr schema;
    size_t delayInMS;
    size_t bufferSize;
    NES::FormatTypes format;
    SourceType type;
    SchemaType generator;
    unsigned long numberOfBuffers;
    bool print;

    using Result = boost::outcome_v2::result<Options, std::string>;
    static Result fromCLI(int argc, char** argv);
};

#endif//NES_OPTIONS_H
