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

#ifndef NES_NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_SOURCEDESCRIPTORASSIGNMENTPHASE_HPP_
#define NES_NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_SOURCEDESCRIPTORASSIGNMENTPHASE_HPP_

#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
namespace NES {

class PhysicalSource;
using PhysicalSourcePtr = std::shared_ptr<PhysicalSource>;

namespace QueryCompilation {

/**
 * @brief Simple phase to add scan and emit operator to pipelines in necessary.
 * A common case would be that, the pipelining phase placed a filter operator in an own pipeline.
 * In this case, the SourceDescriptorAssignmentPhase adds a scan before and end emit operator respectively after the filter operator.
 */
class SourceDescriptorAssignmentPhase {
  public:
    static std::shared_ptr<SourceDescriptorAssignmentPhase> create();
    PipelineQueryPlanPtr apply(const PipelineQueryPlanPtr& queryPlan, const std::vector<PhysicalSourcePtr>& physicalSources);
    /**
     * @brief Create Actual Source descriptor from default source descriptor and Physical source properties
     * @param defaultSourceDescriptor: the default source descriptor
     * @param physicalSource : the physical source
     * @return Shared pointer for actual source descriptor
     */
    SourceDescriptorPtr createSourceDescriptor(SchemaPtr schema, PhysicalSourcePtr physicalSource);
};
}// namespace QueryCompilation
}// namespace NES

#endif//NES_NES_CORE_INCLUDE_QUERYCOMPILER_PHASES_SOURCEDESCRIPTORASSIGNMENTPHASE_HPP_
