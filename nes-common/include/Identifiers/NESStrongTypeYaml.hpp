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

#ifndef NES_COMMON_INCLUDE_UTIL_NESSTRONGTYPEYAML_HPP_
#define NES_COMMON_INCLUDE_UTIL_NESSTRONGTYPEYAML_HPP_
#include <Identifiers/NESStrongType.hpp>
#include <Util/yaml/Yaml.hpp>

/**
 * Adds NESStrongType overloads for the yaml library.
 * This allows assignements of Yaml values with identifiers. This also directly enables Identifiers to be used within the
 * Configuration classes, e.g. `ScalarOption<WorkerId>`
 */
namespace Yaml {
namespace impl {
template<NES::NESIdentifier T>
struct StringConverter<T> {
    static T Get(const std::string& data) {
        typename T::Underlying type;
        std::stringstream ss(data);
        ss >> type;
        return T(type);
    }

    static T Get(const std::string& data, const T& defaultValue) {
        typename T::Underlying type;
        std::stringstream ss(data);
        ss >> type;

        if (ss.fail()) {
            return defaultValue;
        }

        return T(type);
    }
};
}// namespace impl
}// namespace Yaml

#endif//NES_COMMON_INCLUDE_UTIL_NESSTRONGTYPEYAML_HPP_
