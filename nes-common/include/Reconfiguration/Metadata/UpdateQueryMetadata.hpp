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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_UPDATEQUERYMETADATA_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_UPDATEQUERYMETADATA_HPP_

#include <Identifiers/Identifiers.hpp>
#include <Reconfiguration/Metadata/ReconfigurationMetadata.hpp>

namespace NES {

/**
 * @brief Metadata defining the information about the new decomposed plan that needs to be started instead of the current one. This
 * metadata can be used for updating the query plan due to addition, removal, or updation of an operator.
 */
class UpdateQueryMetadata : public ReconfigurationMetadata {

  public:
    UpdateQueryMetadata(WorkerId workerId,
                        SharedQueryId sharedQueryId,
                        DecomposedQueryId decomposedQueryId,
                        DecomposedQueryPlanVersion decomposedQueryPlanVersion)
        : workerId(workerId), sharedQueryId(sharedQueryId), decomposedQueryId(decomposedQueryId),
          decomposedQueryPlanVersion(decomposedQueryPlanVersion){};

    const WorkerId workerId;
    const SharedQueryId sharedQueryId;
    const DecomposedQueryId decomposedQueryId;
    const DecomposedQueryPlanVersion decomposedQueryPlanVersion;
};
}// namespace NES

#endif// NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_UPDATEQUERYMETADATA_HPP_
