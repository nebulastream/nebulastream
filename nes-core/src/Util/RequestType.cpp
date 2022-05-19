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

#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/RequestType.hpp>

namespace NES {

std::string RequestType::toString(const Value queryStatus) {
    switch (queryStatus) {
        case Add: return "ADD";
        case Stop: return "STOP";
        case Fail: return "FAIL";
        case Restart: return "RESTART";
        case Migrate: return "MIGRATE";
        case Update: return "UPDATE";
    }
}

RequestType::Value RequestType::getFromString(const std::string queryStatus) {
    if (queryStatus == "ADD") {
        return Add;
    } else if (queryStatus == "STOP") {
        return Stop;
    } else if (queryStatus == "FAIL") {
        return Fail;
    } else if (queryStatus == "RESTART") {
        return Restart;
    } else if (queryStatus == "MIGRATE") {
        return Migrate;
    } else if (queryStatus == "UPDATE") {
        return Update;
    } else {
        NES_ERROR("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
}

}// namespace NES