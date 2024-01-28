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

#ifndef QUERYPLANEXPORTER_HPP
#define QUERYPLANEXPORTER_HPP
#include <Catalogs/Source/SourceCatalog.hpp>
#include <Identifiers.hpp>
#include <Operators/LogicalOperators/Network/NetworkSinkDescriptor.hpp>
#include <Operators/LogicalOperators/Network/NetworkSourceDescriptor.hpp>
#include <Stage/QueryPipeliner.hpp>
#include <map>
namespace NES::Unikernel::Export {
class QueryPlanExporter {

    NES::Catalogs::Source::SourceCatalogPtr sourceCatalog;

  public:
    explicit QueryPlanExporter(const NES::Catalogs::Source::SourceCatalogPtr& sourceCatalog) : sourceCatalog(sourceCatalog) {}

    struct ExportSourceDescriptor {
        NES::SourceDescriptorPtr sourceDescriptor;
        NES::OriginId originId;
    };

    struct ExportSinkDescriptor {
        std::vector<NES::PipelineId> predecessor;
        NES::Network::NetworkSinkDescriptorPtr descriptor;
    };

    struct Result {
        std::unordered_map<NES::PipelineId, ExportSourceDescriptor> sourcesByPipeline;
        std::unordered_map<NES::PipelineId, ExportSinkDescriptor> sinksByPipeline;
    };

  private:
    [[nodiscard]] std::unordered_map<NES::PipelineId, ExportSinkDescriptor>
    getSinks(const QueryPipeliner::Result& pipeliningResult) const;

    [[nodiscard]] std::unordered_map<NES::PipelineId, ExportSourceDescriptor>
    getSources(const QueryPipeliner::Result& pipeliningResult) const;

  public:
    [[nodiscard]] Result exportQueryPlan(const QueryPipeliner::Result& result) const;
};

}// namespace NES::Unikernel::Export
#endif//QUERYPLANEXPORTER_HPP
