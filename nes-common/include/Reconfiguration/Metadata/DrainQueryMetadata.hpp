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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_DRAINQUERYMETADATA_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_DRAINQUERYMETADATA_HPP_

#include <Reconfiguration/Metadata/ReconfigurationMetadata.hpp>

namespace NES {
/**
 * @brief Metadata defining the information about how many sources will send the reconfiguration marker before draining and
 * terminating the decomposed query.
 */
class DrainQueryMetadata : public ReconfigurationMetadata {
  public:
    DrainQueryMetadata(uint16_t numberOfSources) : numberOfSources(numberOfSources){};
    const uint16_t numberOfSources;
};
}// namespace NES

#endif// NES_COMMON_INCLUDE_RECONFIGURATION_METADATA_DRAINQUERYMETADATA_HPP_
