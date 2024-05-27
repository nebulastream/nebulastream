//
// Created by ls on 28.09.23.
//

#ifndef NES_OPTIONS_H
#define NES_OPTIONS_H

#include <filesystem>
#include <Identifiers/Identifiers.hpp>
#include <Result.hpp>
#include <Sinks/Formats/FormatType.hpp>
#include <YAMLModel.h>
#include <argumentum/argparse.h>

struct Options {
    NES::WorkerId nodeId;
    NES::SharedQueryId sharedQueryId;
    NES::DecomposedQueryPlanId decomposedQueryPlanId;
    NES::OperatorId operatorId;
    NES::WorkerId workerId;
    std::string hostIp = "127.0.0.1";
    uint32_t port = 8082;
    std::string downstreamIp = "127.0.0.1";
    uint32_t downstreamPort = 8080;
    NES::WorkerId downstreamId;
    NES::OriginId originId;
    NES::PartitionId partitionId;
    NES::SubpartitionId subPartitionId;
    NES::SchemaPtr schema;
    size_t delayInMS;
    size_t bufferSize;
    NES::FormatTypes format;
    SchemaType schemaType;
    SourceType type;
    DataSourceType dataSource;
    std::filesystem::path path;
    unsigned long numberOfBuffers;
    bool print;

    using Result = Result<Options, std::string>;
    static Result fromCLI(int argc, char** argv);
    NES::SchemaPtr getSchema() const;
};

#endif//NES_OPTIONS_H
