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

#include <Catalogs/Query/QueryStatus.hpp>
#include <Exceptions/InvalidArgumentException.hpp>
#include <Util/Logger.hpp>
#include <string>
namespace NES {

std::string toString(const QueryStatus queryStatus) {
    switch (queryStatus) {
        case Registered: return "REGISTERED";
        case Scheduling: return "SCHEDULING";
        case Running: return "RUNNING";
        case MarkedForStop: return "MARKED_FOR_STOP";
        case Stopped: return "STOPPED";
        case Failed: return "FAILED";
        case Restarting: return "RESTARTING";
        case Migrating: return "MIGRATING";
    }
};

QueryStatus stringToQueryStatusMap(std::string queryStatus) {
    if (queryStatus == "REGISTERED") {
        return Registered;
    } else if (queryStatus == "SCHEDULING") {
        return Scheduling;
    } else if (queryStatus == "RUNNING") {
        return Running;
    } else if (queryStatus == "MARKED_FOR_STOP") {
        return MarkedForStop;
    } else if (queryStatus == "STOPPED") {
        return Stopped;
    } else if (queryStatus == "FAILED") {
        return Failed;
    } else if (queryStatus == "MIGRATING") {
        return Migrating;
    } else {
        NES_ERROR("No valid query status to parse");
        throw InvalidArgumentException("status", queryStatus);
    }
};

}// namespace NES
