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

#include <Util/LineageType.hpp>
#include <Util/Logger/Logger.hpp>
#include <string>

namespace NES {

LineageType::Value LineageType::getFromString(const std::string lineageType) {
    if (lineageType == "NONE") {
        return LineageType::NONE;
    } else if (lineageType == "IN_MEMORY") {
        return LineageType::IN_MEMORY;
    } else if (lineageType == "PERSISTENT") {
        return LineageType::PERSISTENT;
    } else if (lineageType == "REMOTE") {
        return LineageType::REMOTE;
    } else {
        NES_THROW_RUNTIME_ERROR("LineageType not supported " + lineageType);
    }
}
std::string LineageType::toString(const Value lineageType) {
    switch (lineageType) {
        case Value::NONE: return "NONE";
        case Value::IN_MEMORY: return "IN_MEMORY";
        case Value::PERSISTENT: return "PERSISTENT";
        case Value::REMOTE: return "REMOTE";
        case Value::INVALID: return "INVALID";
    }
}

}// namespace NES
