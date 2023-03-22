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
        case Value::Add: return "ADD";
        case Value::Stop: return "STOP";
        case Value::Fail: return "FAIL";
        case Value::Restart: return "RESTART";
        case Value::Migrate: return "MIGRATE";
        case Value::Update: return "UPDATE";
    }
}

RequestType::Value RequestType::getFromString(const std::string queryStatus) {
    if (queryStatus == "ADD") {
        return Value::Add;
    } else if (queryStatus == "STOP") {
        return Value::Stop;
    } else if (queryStatus == "FAIL") {
        return Value::Fail;
    } else if (queryStatus == "RESTART") {
        return Value::Restart;
    } else if (queryStatus == "MIGRATE") {
        return Value::Migrate;
    } else if (queryStatus == "UPDATE") {
        return Value::Update;
    } else {
        NES_ERROR2("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
}

}// namespace NES