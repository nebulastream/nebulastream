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

#ifndef NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKERSERIALIZATIONUTIL_HPP_
#define NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKERSERIALIZATIONUTIL_HPP_

#include <memory>

namespace NES {

class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;

class SerializableReconfigurationMarker;

class ReconfigurationMarkerSerializationUtil {

  public:
    /**
     * @brief Serialize reconfiguration marker
     * @param reconfigurationMarker : marker to be serialized
     * @param serializableReconfigurationMarker : serialized marker
     */
    static void serialize(const ReconfigurationMarkerPtr& reconfigurationMarker,
                          SerializableReconfigurationMarker& serializableReconfigurationMarker);

    /**
     * @brief Deserialize reconfiguration marker
     * @param serializableReconfigurationMarker : serialized marker that needs to be deserialized
     * @param reconfigurationMarker : deserialized marker
     */
    static void deserialize(const SerializableReconfigurationMarker& serializableReconfigurationMarker,
                            ReconfigurationMarkerPtr reconfigurationMarker);

  private:
    /**
     * @brief Convert pair of plan id and plan version to string
     * @param first : plan id
     * @param second : plan version
     */
    static std::string packDecomposedPlanAsString(uint64_t first, uint32_t second);

    /**
     * @brief Convert string back to a pair of plan id and plan version
     * @param key : plan id and plan version in format "{plan_id},{plan_version}"
     */
    static DecomposedQueryIdWithVersion unpackDecomposedPlanFromString(const std::string& key);
};
}// namespace NES

#endif// NES_COMMON_INCLUDE_RECONFIGURATION_RECONFIGURATIONMARKERSERIALIZATIONUTIL_HPP_
