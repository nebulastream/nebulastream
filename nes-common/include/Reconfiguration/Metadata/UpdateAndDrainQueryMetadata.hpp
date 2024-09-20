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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_UPDATEANDDRAINQUERYMETADATA_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_UPDATEANDDRAINQUERYMETADATA_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/Metadata/ReconfigurationMetadata.hpp>

namespace NES {

/**
 * @brief Metadata defining the information about the new decomposed plan that needs to be started before terminating it. This
 * metadata can be used for doing state transmission before terminating the decomposed query.
 */
class UpdateAndDrainQueryMetadata : public ReconfigurationMetadata {
  public:
    UpdateAndDrainQueryMetadata(WorkerId workerId,
                                SharedQueryId sharedQueryId,
                                DecomposedQueryId decomposedQueryId,
                                DecomposedQueryPlanVersion decomposedQueryPlanVersion,
                                uint16_t numberOfSources)
        : workerId(workerId), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId),
          decomposedQueryPlanVersion(decomposedQueryPlanVersion), numberOfSources(numberOfSources){};

    const WorkerId workerId;
    const SharedQueryId sharedQueryId;
    const DecomposedQueryId decomposedQueryId;
    const DecomposedQueryPlanVersion decomposedQueryPlanVersion;
    const uint16_t numberOfSources;
};
}// namespace NES

#endif// NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_UPDATEANDDRAINQUERYMETADATA_HPP_
