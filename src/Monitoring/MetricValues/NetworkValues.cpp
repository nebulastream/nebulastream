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
#include <API/AttributeField.hpp>
#include <API/Schema.hpp>
#include <Common/DataTypes/FixedChar.hpp>
#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <Monitoring/Util/MetricUtils.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayout.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicRowLayoutField.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger.hpp>
#include <Util/UtilityFunctions.hpp>
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr NetworkValues::getSchema(const std::string& prefix) {
    DataTypePtr intNameField = std::make_shared<FixedChar>(20);

    SchemaPtr schema = Schema::create()
                           ->addField(prefix + "name", BasicType::UINT64)
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

NetworkValues NetworkValues::fromBuffer(const SchemaPtr& schema, Runtime::TupleBuffer& buf, const std::string& prefix) {
    NetworkValues output{};
    auto i = schema->getIndex(prefix);

    if (buf.getNumberOfTuples() > 1) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Tuple size should be 1, but is larger "
                                + std::to_string(buf.getNumberOfTuples()));
    }

    if (!MetricUtils::validateFieldsInSchema(NetworkValues::getSchema(""), schema, i)) {
        NES_THROW_RUNTIME_ERROR("NetworkValues: Incomplete number of fields in schema.");
    }
    auto layout = Runtime::DynamicMemoryLayout::DynamicRowLayout::create(schema, true);
    auto bindedRowLayout = layout->bind(buf);

    output.interfaceName = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rBytes = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rPackets = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rErrs = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rDrop = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rFifo = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rFrame = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rCompressed = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.rMulticast = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];

    output.tBytes = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tPackets = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tErrs = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tDrop = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tFifo = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tColls = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tCarrier = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i++, bindedRowLayout)[0];
    output.tCompressed = Runtime::DynamicMemoryLayout::DynamicRowLayoutField<uint64_t, true>::create(i, bindedRowLayout)[0];

    return output;
}

web::json::value NetworkValues::toJson() const {
    web::json::value metricsJson{};

    metricsJson["R_BYTES"] = web::json::value::number(rBytes);
    metricsJson["R_PACKETS"] = web::json::value::number(rPackets);
    metricsJson["R_ERRS"] = web::json::value::number(rErrs);
    metricsJson["R_DROP"] = web::json::value::number(rDrop);
    metricsJson["R_FIFO"] = web::json::value::number(rFifo);
    metricsJson["R_FRAME"] = web::json::value::number(rFrame);
    metricsJson["R_COMPRESSED"] = web::json::value::number(rCompressed);
    metricsJson["R_MULTICAST"] = web::json::value::number(rMulticast);

    metricsJson["T_BYTES"] = web::json::value::number(tBytes);
    metricsJson["T_PACKETS"] = web::json::value::number(tPackets);
    metricsJson["T_ERRS"] = web::json::value::number(tErrs);
    metricsJson["T_DROP"] = web::json::value::number(tDrop);
    metricsJson["T_FIFO"] = web::json::value::number(tFifo);
    metricsJson["T_COLLS"] = web::json::value::number(tColls);
    metricsJson["T_CARRIER"] = web::json::value::number(tCarrier);
    metricsJson["T_COMPRESSED"] = web::json::value::number(tCompressed);

    return metricsJson;
}

void writeToBuffer(const NetworkValues& metric, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(NetworkValues) < buf.getBufferSize(), "NetworkValues: Content does not fit in TupleBuffer");

    memcpy(tbuffer + byteOffset, &metric, sizeof(NetworkValues));
    buf.setNumberOfTuples(1);
}

bool NetworkValues::operator==(const NetworkValues& rhs) const {
    return interfaceName == rhs.interfaceName && rBytes == rhs.rBytes && rPackets == rhs.rPackets && rErrs == rhs.rErrs
        && rDrop == rhs.rDrop && rFifo == rhs.rFifo && rFrame == rhs.rFrame && rCompressed == rhs.rCompressed
        && rMulticast == rhs.rMulticast && tBytes == rhs.tBytes && tPackets == rhs.tPackets && tErrs == rhs.tErrs
        && tDrop == rhs.tDrop && tFifo == rhs.tFifo && tColls == rhs.tColls && tCarrier == rhs.tCarrier
        && tCompressed == rhs.tCompressed;
}
bool NetworkValues::operator!=(const NetworkValues& rhs) const { return !(rhs == *this); }

}// namespace NES
