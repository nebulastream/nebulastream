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
#ifndef NES_COMMON_INCLUDE_IDENTIFIERS_DECOMPOSEDQUERYIDWITHVERSION_HPP_
#define NES_COMMON_INCLUDE_IDENTIFIERS_DECOMPOSEDQUERYIDWITHVERSION_HPP_

#include <Identifiers/Identifiers.hpp>
#include <functional>

namespace NES {
/**
* @brief Wrapper for decomposed query plan and version
*/
class DecomposedQueryIdWithVersion {
  public:
    DecomposedQueryIdWithVersion(DecomposedQueryId id, DecomposedQueryPlanVersion version) : id(id), version(version) {}
    DecomposedQueryIdWithVersion(uint64_t id, uint16_t version)
        : id(DecomposedQueryId(id)), version(DecomposedQueryPlanVersion(version)) {}
    DecomposedQueryIdWithVersion() : id(INVALID_DECOMPOSED_QUERY_PLAN_ID) {}

    bool operator==(DecomposedQueryIdWithVersion const& other) const { return (id == other.id && version == other.version); }

    bool operator<(const DecomposedQueryIdWithVersion& other) const {
        if (id != other.id) {
            return id < other.id;
        }
        return version < other.version;
    }

    DecomposedQueryId id;
    DecomposedQueryPlanVersion version;
};
}// namespace NES

namespace std {
template<>
struct hash<NES::DecomposedQueryIdWithVersion> {
    size_t operator()(const NES::DecomposedQueryIdWithVersion& decomposedQueryIdWithVersion) const {
        size_t hash1 = hash<NES::DecomposedQueryId>{}(decomposedQueryIdWithVersion.id);
        size_t hash2 = hash<NES::DecomposedQueryPlanVersion>{}(decomposedQueryIdWithVersion.version);

        return hash1 ^ (hash2 + 0x9e3779b9 + (hash1 << 6) + (hash1 >> 2));
    }
};
}// namespace std
#endif// NES_COMMON_INCLUDE_IDENTIFIERS_DECOMPOSEDQUERYIDWITHVERSION_HPP_
