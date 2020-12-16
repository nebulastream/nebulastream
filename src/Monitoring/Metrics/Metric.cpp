/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>

namespace NES {

void serialize(uint64_t metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->addField(prefix, BasicType::UINT64);
    buf.setNumberOfTuples(1);
    auto layout = createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric);
}

void serialize(const std::string& metric, std::shared_ptr<Schema> schema, TupleBuffer&, const std::string& prefix) {
    NES_THROW_RUNTIME_ERROR("Metric: Serialization for std::string not possible for metric " + metric + "and schema " + prefix
                            + schema->toString());
}

}// namespace NES