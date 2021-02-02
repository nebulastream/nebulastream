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

#include <API/Schema.hpp>
#include <Monitoring/MetricValues/DiscMetrics.hpp>
#include <Monitoring/Metrics/MonitoringPlan.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

SchemaPtr DiskMetrics::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "F_BSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_FRSIZE", BasicType::UINT64)
                           ->addField(prefix + "F_BLOCKS", BasicType::UINT64)
                           ->addField(prefix + "F_BFREE", BasicType::UINT64)
                           ->addField(prefix + "F_BAVAIL", BasicType::UINT64);
    return schema;
}

void serialize(const DiskMetrics& metric, SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->copyFields(DiskMetrics::getSchema(prefix));

    auto layout = NodeEngine::createRowLayout(schema);
    layout->getValueField<uint64_t>(0, noFields)->write(buf, metric.fBsize);
    layout->getValueField<uint64_t>(0, noFields + 1)->write(buf, metric.fFrsize);
    layout->getValueField<uint64_t>(0, noFields + 2)->write(buf, metric.fBlocks);
    layout->getValueField<uint64_t>(0, noFields + 3)->write(buf, metric.fBfree);
    layout->getValueField<uint64_t>(0, noFields + 4)->write(buf, metric.fBavail);
    buf.setNumberOfTuples(1);
}

DiskMetrics DiskMetrics::fromBuffer(SchemaPtr schema, NodeEngine::TupleBuffer& buf, const std::string& prefix) {
    DiskMetrics output{};
    auto i = schema->getIndex(prefix);

    if (i >= schema->getSize()) {
        NES_THROW_RUNTIME_ERROR("DiskMetrics: Prefix " + prefix + " could not be found in schema:\n" + schema->toString());
    }
    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("DiskMetrics: Tuple size should be 1, but is " + std::to_string(buf.getNumberOfTuples()));
    }
    if (!(UtilityFunctions::endsWith(schema->fields[i]->name, "F_BSIZE")
          && UtilityFunctions::endsWith(schema->fields[i + 4]->name, "F_BAVAIL"))) {
        NES_THROW_RUNTIME_ERROR("DiskMetrics: Missing fields in schema.");
    }

    auto layout = NodeEngine::createRowLayout(schema);

    output.fBsize = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.fFrsize = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.fBlocks = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.fBfree = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.fBavail = layout->getValueField<uint64_t>(0, i)->read(buf);
    return output;
}

}// namespace NES