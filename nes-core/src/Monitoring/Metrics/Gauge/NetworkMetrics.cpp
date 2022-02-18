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
#include "Monitoring/Metrics/Gauge/NetworkMetrics.hpp"
#include "API/AttributeField.hpp"
#include "API/Schema.hpp"
#include "Common/DataTypes/FixedChar.hpp"
#include "Monitoring/Util/MetricUtils.hpp"
#include "Runtime/MemoryLayout/RowLayout.hpp"
#include "Runtime/MemoryLayout/RowLayoutField.hpp"
#include "Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp"
#include "Runtime/TupleBuffer.hpp"
#include "Util/Logger.hpp"
#include "Util/UtilityFunctions.hpp"
#include <cpprest/json.h>
#include <cstring>

namespace NES {

SchemaPtr NetworkMetrics::getSchema(const std::string& prefix) {
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

void NetworkMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t byteOffset) const {
    auto* tbuffer = buf.getBuffer<uint8_t>();
    NES_ASSERT(byteOffset + sizeof(NetworkMetrics) <= buf.getBufferSize(), "NetworkMetrics: Content does not fit in TupleBuffer");
    memcpy(tbuffer + byteOffset, this, sizeof(NetworkMetrics));
    buf.setNumberOfTuples(buf.getNumberOfTuples()+1);
}

void NetworkMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    //get index where the schema is starting
    auto schema = getSchema("");
    auto layout = Runtime::MemoryLayouts::RowLayout::create(schema, buf.getBufferSize());

    int cnt = 0;
    interfaceName = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rBytes = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rPackets = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rErrs = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rDrop = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rFifo = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rFrame = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rCompressed = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    rMulticast = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];

    tBytes = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tPackets = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tErrs = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tDrop = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tFifo = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tColls = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tCarrier = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
    tCompressed = Runtime::MemoryLayouts::RowLayoutField<uint64_t, true>::create(byteOffset + cnt++, layout, buf)[0];
}

web::json::value NetworkMetrics::toJson() const {
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

bool NetworkMetrics::operator==(const NetworkMetrics& rhs) const {
    return interfaceName == rhs.interfaceName && rBytes == rhs.rBytes && rPackets == rhs.rPackets && rErrs == rhs.rErrs
        && rDrop == rhs.rDrop && rFifo == rhs.rFifo && rFrame == rhs.rFrame && rCompressed == rhs.rCompressed
        && rMulticast == rhs.rMulticast && tBytes == rhs.tBytes && tPackets == rhs.tPackets && tErrs == rhs.tErrs
        && tDrop == rhs.tDrop && tFifo == rhs.tFifo && tColls == rhs.tColls && tCarrier == rhs.tCarrier
        && tCompressed == rhs.tCompressed;
}
bool NetworkMetrics::operator!=(const NetworkMetrics& rhs) const { return !(rhs == *this); }

void writeToBuffer(const NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    metrics.writeToBuffer(buf, byteOffset);
}

void readFromBuffer(NetworkMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t byteOffset) {
    metrics.readFromBuffer(buf, byteOffset);
}

}// namespace NES
