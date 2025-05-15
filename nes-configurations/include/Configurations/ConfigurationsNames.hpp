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
#pragma once
#include <cstdint>
#include <string>

using namespace std::string_literals;

namespace NES::Configurations
{

enum class InputFormat : uint8_t
{
    CSV,
    JSON
};
/// NOLINTBEGIN(cert-err58-cpp)
///Coordinator Configuration Names
const std::string CONFIG_PATH = "configPath";

///Configuration names for source types (Descriptor(.hpp) must support these in 'validateAndFormat()')
const std::string SOURCE_TYPE_CONFIG = "type";
const std::string NUMBER_OF_BUFFERS_IN_LOCAL_POOL = "numberOfBuffersInLocalPool";
/// NOLINTEND(cert-err58-cpp)

}
