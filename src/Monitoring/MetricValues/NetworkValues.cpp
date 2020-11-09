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
#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <NodeEngine/MemoryLayout/RowLayout.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>

namespace NES {

SchemaPtr NetworkValues::getSchema(const std::string& prefix) {
    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "rBytes", BasicType::UINT64)
                           ->addField(prefix + "rPackets", BasicType::UINT64)
                           ->addField(prefix + "rErrs", BasicType::UINT64)
                           ->addField(prefix + "rDrop", BasicType::UINT64)
                           ->addField(prefix + "rFifo", BasicType::UINT64)
                           ->addField(prefix + "rFrame", BasicType::UINT64)
                           ->addField(prefix + "rCompressed", BasicType::UINT64)
                           ->addField(prefix + "rMulticast", BasicType::UINT64)

                           ->addField(prefix + "tBytes", BasicType::UINT64)
                           ->addField(prefix + "tPackets", BasicType::UINT64)
                           ->addField(prefix + "tErrs", BasicType::UINT64)
                           ->addField(prefix + "tDrop", BasicType::UINT64)
                           ->addField(prefix + "tFifo", BasicType::UINT64)
                           ->addField(prefix + "tColls", BasicType::UINT64)
                           ->addField(prefix + "tCarrier", BasicType::UINT64)
                           ->addField(prefix + "tCompressed", BasicType::UINT64);

    return schema;
}

NetworkValues NetworkValues::fromBuffer(SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    NetworkValues output{};
    auto i = schema->getIndex(prefix);

    if (i >= schema->getSize()) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Prefix " + prefix + " could not be found in schema:\n" + schema->toString());
    }
    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Tuple size should be 1, but is " + std::to_string(buf.getNumberOfTuples()));
    }
    if (!(UtilityFunctions::endsWith(schema->fields[i]->name, "rBytes")
          && UtilityFunctions::endsWith(schema->fields[i + 15]->name, "tCompressed"))) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Missing fields in schema.");
    }

    auto layout = createRowLayout(schema);

    output.rBytes = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rPackets = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rErrs = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rDrop = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rFifo = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rFrame = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rCompressed = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.rMulticast = layout->getValueField<uint64_t>(0, i++)->read(buf);

    output.tBytes = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tPackets = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tErrs = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tDrop = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tFifo = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tColls = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tCarrier = layout->getValueField<uint64_t>(0, i++)->read(buf);
    output.tCompressed = layout->getValueField<uint64_t>(0, i++)->read(buf);

    return output;
}

void serialize(const NetworkValues& metric, SchemaPtr schema, TupleBuffer& buf, const std::string& prefix) {
    auto noFields = schema->getSize();
    schema->copyFields(NetworkValues::getSchema(prefix));

    auto layout = createRowLayout(schema);
    //get buffer location to write to from the layout and write the specific value to it
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rBytes);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rPackets);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rErrs);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rDrop);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rFifo);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rFrame);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rCompressed);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.rMulticast);

    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tBytes);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tPackets);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tErrs);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tDrop);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tFifo);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tColls);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tCarrier);
    layout->getValueField<uint64_t>(0, noFields++)->write(buf, metric.tCompressed);

    buf.setNumberOfTuples(1);
}

}// namespace NES
