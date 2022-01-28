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

#include <Monitoring/Metrics/Metric.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <cstring>

namespace NES {

void writeToBuffer(uint64_t metric, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(uint64_t) < buf.getBufferSize(), "Metric: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metric, sizeof(uint64_t));
    buf.setNumberOfTuples(1);
}

void writeToBuffer(const std::string& metric, Runtime::TupleBuffer&, uint64_t) {
    NES_THROW_RUNTIME_ERROR("Metric: Serialization for std::string not possible for metric " + metric);
}

SchemaPtr getSchema(uint64_t, const std::string& prefix) { return Schema::create()->addField(prefix + "uint64", UINT64); }

SchemaPtr getSchema(const std::string& metric, const std::string& prefix) {
    NES_THROW_RUNTIME_ERROR("Metric: Schema for std::string not possible for metric " + metric + "and prefix " + prefix);
}

}// namespace NES