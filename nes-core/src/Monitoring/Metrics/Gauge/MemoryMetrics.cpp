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

#include <Monitoring/Metrics/Gauge/MemoryMetrics.hpp>

#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

MemoryMetrics::MemoryMetrics()
    : nodeId(0), TOTAL_RAM(0), TOTAL_SWAP(0), FREE_RAM(0), SHARED_RAM(0), BUFFER_RAM(0), FREE_SWAP(0), TOTAL_HIGH(0),
      FREE_HIGH(0), PROCS(0), MEM_UNIT(0), LOADS_1MIN(0), LOADS_5MIN(0), LOADS_15MIN(0), schema(getDefaultSchema("")) {}

MemoryMetrics::MemoryMetrics(SchemaPtr schema)
    : nodeId(0), TOTAL_RAM(0), TOTAL_SWAP(0), FREE_RAM(0), SHARED_RAM(0), BUFFER_RAM(0), FREE_SWAP(0), TOTAL_HIGH(0),
      FREE_HIGH(0), PROCS(0), MEM_UNIT(0), LOADS_1MIN(0), LOADS_5MIN(0), LOADS_15MIN(0), schema(std::move(schema)) {}

SchemaPtr MemoryMetrics::getDefaultSchema(const std::string& prefix) {
    auto schema = Schema::create(Schema::ROW_LAYOUT)
                      ->addField(prefix + "node_id", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_RAM", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_SWAP", BasicType::UINT64)
                      ->addField(prefix + "FREE_RAM", BasicType::UINT64)
                      ->addField(prefix + "SHARED_RAM", BasicType::UINT64)
                      ->addField(prefix + "BUFFER_RAM", BasicType::UINT64)
                      ->addField(prefix + "FREE_SWAP", BasicType::UINT64)
                      ->addField(prefix + "TOTAL_HIGH", BasicType::UINT64)
                      ->addField(prefix + "FREE_HIGH", BasicType::UINT64)
                      ->addField(prefix + "PROCS", BasicType::UINT64)
                      ->addField(prefix + "MEM_UNIT", BasicType::UINT64)
                      ->addField(prefix + "LOADS_1MIN", BasicType::UINT64)
                      ->addField(prefix + "LOADS_5MIN", BasicType::UINT64)
                      ->addField(prefix + "LOADS_15MIN", BasicType::UINT64);
    return schema;
}

SchemaPtr MemoryMetrics::createSchema(const std::string& prefix, const std::list<std::string>& configuredMetrics) {
    SchemaPtr schema = Schema::create(Schema::ROW_LAYOUT)
                           ->addField(prefix + "node_id", BasicType::UINT64);

    for (const auto& metric : configuredMetrics) {
        if (metric == "TOTAL_RAM") {
            schema->addField(prefix + "TOTAL_RAM", BasicType::UINT64);
        } else if (metric == "TOTAL_SWAP") {
            schema->addField(prefix + "TOTAL_SWAP", BasicType::UINT64);
        } else if (metric == "FREE_RAM") {
            schema->addField(prefix + "FREE_RAM", BasicType::UINT64);
        } else if (metric == "SHARED_RAM") {
            schema->addField(prefix + "SHARED_RAM", BasicType::UINT64);
        } else if (metric == "BUFFER_RAM") {
            schema->addField(prefix + "BUFFER_RAM", BasicType::UINT64);
        } else if (metric == "FREE_SWAP") {
            schema->addField(prefix + "FREE_SWAP", BasicType::UINT64);
        } else if (metric == "TOTAL_HIGH") {
            schema->addField(prefix + "TOTAL_HIGH", BasicType::UINT64);
        } else if (metric == "FREE_HIGH") {
            schema->addField(prefix + "FREE_HIGH", BasicType::UINT64);
        } else if (metric == "PROCS") {
            schema->addField(prefix + "PROCS", BasicType::UINT64);
        } else if (metric == "MEM_UNIT") {
            schema->addField(prefix + "MEM_UNIT", BasicType::UINT64);
        } else if (metric == "LOADS_1MIN") {
            schema->addField(prefix + "LOADS_1MIN", BasicType::UINT64);
        } else if (metric == "LOADS_5MIN") {
            schema->addField(prefix + "LOADS_5MIN", BasicType::UINT64);
        } else if (metric == "LOADS_15MIN") {
            schema->addField(prefix + "LOADS_15MIN", BasicType::UINT64);
        } else {
            NES_INFO("DiskMetrics: Metric unknown: " << metric);
        }
    }
    return schema;
}

void MemoryMetrics::setSchema(SchemaPtr newSchema) { this->schema = std::move(newSchema); }

SchemaPtr MemoryMetrics::getSchema() const { return this->schema; }

std::vector<std::string> MemoryMetrics::getAttributesVector() {
    std::vector<std::string> attributesVector { "TOTAL_RAM", "TOTAL_SWAP", "FREE_RAM", "SHARED_RAM", "BUFFER_RAM", "FREE_SWAP",
                                              "TOTAL_HIGH", "FREE_HIGH", "PROCS", "MEM_UNIT", "LOADS_1MIN", "LOADS_5MIN",
                                              "LOADS_15MIN"};
    return attributesVector;
}

uint64_t MemoryMetrics::getValue(const std::string& metricName) const {
    if (metricName == "TOTAL_RAM") {
        return TOTAL_RAM;
    } else if (metricName == "TOTAL_SWAP") {
        return TOTAL_SWAP;
    } else if (metricName == "FREE_RAM") {
        return FREE_RAM;
    } else if (metricName == "SHARED_RAM") {
        return SHARED_RAM;
    } else if (metricName == "BUFFER_RAM") {
        return BUFFER_RAM;
    } else if (metricName == "FREE_SWAP") {
        return FREE_SWAP;
    } else if (metricName == "TOTAL_HIGH") {
        return TOTAL_HIGH;
    } else if (metricName == "FREE_HIGH") {
        return FREE_HIGH;
    } else if (metricName == "PROCS") {
        return PROCS;
    } else if (metricName == "MEM_UNIT") {
        return MEM_UNIT;
    } else if (metricName == "LOADS_1MIN") {
        return LOADS_1MIN;
    } else if (metricName == "LOADS_5MIN") {
        return LOADS_5MIN;
    } else if (metricName == "LOADS_15MIN") {
        return LOADS_15MIN;
    } else if (metricName == "nodeId") {
        return nodeId;
    }else {
        NES_DEBUG("Unknown Metricname: " << metricName);
        throw std::exception();
    }
}

void MemoryMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(this->schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = this->schema->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "MemoryMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);//NodeId has always to be filled
    if (schema->contains("TOTAL_RAM")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(TOTAL_RAM);
    }
    if (schema->contains("TOTAL_SWAP")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(TOTAL_SWAP);
    }
    if (schema->contains("FREE_RAM")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(FREE_RAM);
    }
    if (schema->contains("SHARED_RAM")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(SHARED_RAM);
    }
    if (schema->contains("BUFFER_RAM")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(BUFFER_RAM);
    }
    if (schema->contains("FREE_SWAP")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(FREE_SWAP);
    }
    if (schema->contains("TOTAL_HIGH")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(TOTAL_HIGH);
    }
    if (schema->contains("FREE_HIGH")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(FREE_HIGH);
    }
    if (schema->contains("PROCS")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(PROCS);
    }
    if (schema->contains("MEM_UNIT")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(MEM_UNIT);
    }
    if (schema->contains("LOADS_1MIN")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(LOADS_1MIN);
    }
    if (schema->contains("LOADS_5MIN")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(LOADS_5MIN);
    }
    if (schema->contains("LOADS_15MIN")) {
        buffer[tupleIndex][cnt++].write<uint64_t>(LOADS_15MIN);
    }

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void MemoryMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(this->schema, buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    if (schema->contains("TOTAL_RAM")) {
        TOTAL_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("TOTAL_SWAP")) {
        TOTAL_SWAP = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("FREE_RAM")) {
        FREE_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("SHARED_RAM")) {
        SHARED_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("BUFFER_RAM")) {
        BUFFER_RAM = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("FREE_SWAP")) {
        FREE_SWAP = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("TOTAL_HIGH")) {
        TOTAL_HIGH = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("FREE_HIGH")) {
        FREE_HIGH = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("PROCS")) {
        PROCS = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("MEM_UNIT")) {
        MEM_UNIT = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("LOADS_1MIN")) {
        LOADS_1MIN = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("LOADS_5MIN")) {
        LOADS_5MIN = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
    if (schema->contains("LOADS_15MIN")) {
        LOADS_15MIN = buffer[tupleIndex][cnt++].read<uint64_t>();
    }
}

bool MemoryMetrics::operator==(const MemoryMetrics& rhs) const {
    return nodeId == rhs.nodeId && TOTAL_RAM == rhs.TOTAL_RAM && TOTAL_SWAP == rhs.TOTAL_SWAP && FREE_RAM == rhs.FREE_RAM
        && SHARED_RAM == rhs.SHARED_RAM && BUFFER_RAM == rhs.BUFFER_RAM && FREE_SWAP == rhs.FREE_SWAP
        && TOTAL_HIGH == rhs.TOTAL_HIGH && FREE_HIGH == rhs.FREE_HIGH && PROCS == rhs.PROCS && MEM_UNIT == rhs.MEM_UNIT
        && LOADS_1MIN == rhs.LOADS_1MIN && LOADS_5MIN == rhs.LOADS_5MIN && LOADS_15MIN == rhs.LOADS_15MIN;
}

bool MemoryMetrics::operator!=(const MemoryMetrics& rhs) const { return !(rhs == *this); }

nlohmann::json MemoryMetrics::toJson() const {
    nlohmann::json metricsJson{};
    metricsJson["NODE_ID"] = nodeId;
    if (schema->contains("TOTAL_RAM")) {
        metricsJson["TOTAL_RAM"] = TOTAL_RAM;
    }
    if (schema->contains("TOTAL_SWAP")) {
        metricsJson["TOTAL_SWAP"] = TOTAL_SWAP;
    }
    if (schema->contains("FREE_RAM")) {
        metricsJson["FREE_RAM"] = FREE_RAM;
    }
    if (schema->contains("SHARED_RAM")) {
        metricsJson["SHARED_RAM"] = SHARED_RAM;
    }
    if (schema->contains("BUFFER_RAM")) {
        metricsJson["BUFFER_RAM"] = BUFFER_RAM;
    }
    if (schema->contains("FREE_SWAP")) {
        metricsJson["FREE_SWAP"] = FREE_SWAP;
    }
    if (schema->contains("TOTAL_HIGH")) {
        metricsJson["TOTAL_HIGH"] = TOTAL_HIGH;
    }
    if (schema->contains("FREE_HIGH")) {
        metricsJson["FREE_HIGH"] = FREE_HIGH;
    }
    if (schema->contains("PROCS")) {
        metricsJson["PROCS"] = PROCS;
    }
    if (schema->contains("MEM_UNIT")) {
        metricsJson["MEM_UNIT"] = MEM_UNIT;
    }
    if (schema->contains("LOADS_1MIN")) {
        metricsJson["LOADS_1MIN"] = LOADS_1MIN;
    }
    if (schema->contains("LOADS_5MIN")) {
        metricsJson["LOADS_5MIN"] = LOADS_5MIN;
    }
    if (schema->contains("LOADS_15MIN")) {
        metricsJson["LOADS_15MIN"] = LOADS_15MIN;
    }

    return metricsJson;
}

void writeToBuffer(const MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(MemoryMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const MemoryMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring