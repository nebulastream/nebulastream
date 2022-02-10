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

#include <Runtime/QueryManager.hpp>
#include <Sinks/Mediums/NullOutputSink.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <sstream>
#include <string>

namespace NES {
NullOutputSink::NullOutputSink(Runtime::QueryManagerPtr queryManager, ReplicationServicePtr replicationService, QuerySubPlanId querySubPlanId)
    : SinkMedium(nullptr, std::move(queryManager), querySubPlanId, std::move(replicationService)) {}

NullOutputSink::~NullOutputSink() = default;

SinkMediumTypes NullOutputSink::getSinkMediumType() { return NULL_SINK; }

bool NullOutputSink::writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) {
    NES_DEBUG("NullOutputSink output=" << inputBuffer);

    return true;
}

std::string NullOutputSink::toString() const {
    std::stringstream ss;
    ss << "NULL_SINK(";
    ss << ")";
    return ss.str();
}

void NullOutputSink::setup() {
    // currently not required
}
void NullOutputSink::shutdown() {
    // currently not required
}

}// namespace NES
