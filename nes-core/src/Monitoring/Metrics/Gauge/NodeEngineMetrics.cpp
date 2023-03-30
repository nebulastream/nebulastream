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

#include <Monitoring/Metrics/Gauge/NodeEngineMetrics.hpp>

#include <API/Schema.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Runtime/MemoryLayout/RowLayout.hpp>
#include <Runtime/MemoryLayout/RowLayoutTupleBuffer.hpp>
#include <Util/UtilityFunctions.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

NodeEngineMetrics::NodeEngineMetrics()
    : nodeId(0), processedTasks(0), processedTuple(0), processedBuffers(0), processedWatermarks(0), latencySum(0), queueSizeSum(0),
      availableGlobalBufferSum(0), availableFixedBufferSum(0), timestampQueryStart(0), timestampFirstProcessedTask(0),
      timestampLastProcessedTask(0), queryId(0), subQueryId(0), tsToLatencyMap() {}

SchemaPtr NodeEngineMetrics::getSchema(const std::string& prefix) {
    auto schema = Schema::create(Schema::ROW_LAYOUT)
                      ->addField(prefix + "node_id", BasicType::UINT64)
                      ->addField(prefix + "processedTasks", BasicType::UINT64)
                      ->addField(prefix + "processedTuple", BasicType::UINT64)
                      ->addField(prefix + "processedBuffers", BasicType::UINT64)
                      ->addField(prefix + "processedWatermarks", BasicType::UINT64)
                      ->addField(prefix + "latencySum", BasicType::UINT64)
                      ->addField(prefix + "queueSizeSum", BasicType::UINT64)
                      ->addField(prefix + "availableGlobalBufferSum", BasicType::UINT64)
                      ->addField(prefix + "availableFixedBufferSum", BasicType::UINT64)
                      ->addField(prefix + "timestampQueryStart", BasicType::UINT64)
                      ->addField(prefix + "timestampFirstProcessedTask", BasicType::UINT64)
                      ->addField(prefix + "timestampLastProcessedTask", BasicType::UINT64)
                      ->addField(prefix + "queryId", BasicType::UINT64)
                      ->addField(prefix + "subQueryId", BasicType::UINT64);
                      //->addField(prefix + "tsToLatencyMap", BasicType::MAP);  // TODO: should be added when non BasicType can be handled
    return schema;
}

void NodeEngineMetrics::writeToBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) const {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(NodeEngineMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    auto totalSize = NodeEngineMetrics::getSchema("")->getSchemaSizeInBytes();
    NES_ASSERT(totalSize <= buf.getBufferSize(),
               "NodeEngineMetrics: Content does not fit in TupleBuffer totalSize:" + std::to_string(totalSize) + " < "
                   + " getBufferSize:" + std::to_string(buf.getBufferSize()));

    uint64_t cnt = 0;
    buffer[tupleIndex][cnt++].write<uint64_t>(nodeId);
    buffer[tupleIndex][cnt++].write<uint64_t>(processedTasks);
    buffer[tupleIndex][cnt++].write<uint64_t>(processedTuple);
    buffer[tupleIndex][cnt++].write<uint64_t>(processedBuffers);
    buffer[tupleIndex][cnt++].write<uint64_t>(processedWatermarks);
    buffer[tupleIndex][cnt++].write<uint64_t>(latencySum);
    buffer[tupleIndex][cnt++].write<uint64_t>(queueSizeSum);
    buffer[tupleIndex][cnt++].write<uint64_t>(availableGlobalBufferSum);
    buffer[tupleIndex][cnt++].write<uint64_t>(availableFixedBufferSum);
    buffer[tupleIndex][cnt++].write<uint64_t>(timestampQueryStart);
    buffer[tupleIndex][cnt++].write<uint64_t>(timestampFirstProcessedTask);
    buffer[tupleIndex][cnt++].write<uint64_t>(timestampLastProcessedTask);
    buffer[tupleIndex][cnt++].write<uint64_t>(queryId);
    buffer[tupleIndex][cnt++].write<uint64_t>(subQueryId);
    //buffer[tupleIndex][cnt++].write<uint64_t>(tsToLatencyMap);  // TODO: should be added when non BasicType can be handled

    buf.setNumberOfTuples(buf.getNumberOfTuples() + 1);
}

void NodeEngineMetrics::readFromBuffer(Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    auto layout = Runtime::MemoryLayouts::RowLayout::create(NodeEngineMetrics::getSchema(""), buf.getBufferSize());
    auto buffer = Runtime::MemoryLayouts::DynamicTupleBuffer(layout, buf);

    uint64_t cnt = 0;
    nodeId = buffer[tupleIndex][cnt++].read<uint64_t>();
    processedTasks = buffer[tupleIndex][cnt++].read<uint64_t>();
    processedTuple = buffer[tupleIndex][cnt++].read<uint64_t>();
    processedBuffers = buffer[tupleIndex][cnt++].read<uint64_t>();
    processedWatermarks = buffer[tupleIndex][cnt++].read<uint64_t>();
    latencySum = buffer[tupleIndex][cnt++].read<uint64_t>();
    queueSizeSum = buffer[tupleIndex][cnt++].read<uint64_t>();
    availableGlobalBufferSum = buffer[tupleIndex][cnt++].read<uint64_t>();
    availableFixedBufferSum = buffer[tupleIndex][cnt++].read<uint64_t>();
    timestampQueryStart = buffer[tupleIndex][cnt++].read<uint64_t>();
    timestampFirstProcessedTask = buffer[tupleIndex][cnt++].read<uint64_t>();
    timestampLastProcessedTask = buffer[tupleIndex][cnt++].read<uint64_t>();
    queryId = buffer[tupleIndex][cnt++].read<uint64_t>();
    subQueryId = buffer[tupleIndex][cnt++].read<uint64_t>();
    //tsToLatencyMap = buffer[tupleIndex][cnt++].read<uint64_t>();  // TODO: should be added when non BasicType can be handled
}

SchemaPtr getSchema(const NodeEngineMetrics&, const std::string& prefix) { return NodeEngineMetrics::getSchema(prefix); }

bool NodeEngineMetrics::operator==(const NodeEngineMetrics& rhs) const {
    return nodeId == rhs.nodeId && processedTasks == rhs.processedTasks && processedTuple == rhs.processedTuple
        && processedBuffers == rhs.processedBuffers && processedWatermarks == rhs.processedWatermarks
        && latencySum == rhs.latencySum && queueSizeSum == rhs.queueSizeSum && availableGlobalBufferSum == rhs.availableGlobalBufferSum
        && availableFixedBufferSum == rhs.availableFixedBufferSum && timestampQueryStart == rhs.timestampQueryStart
        && timestampFirstProcessedTask == rhs.timestampFirstProcessedTask && timestampLastProcessedTask == rhs.timestampLastProcessedTask
        && queryId == rhs.queryId && subQueryId == rhs.subQueryId && tsToLatencyMap == rhs.tsToLatencyMap;
}

bool NodeEngineMetrics::operator!=(const NodeEngineMetrics& rhs) const { return !(rhs == *this); }

nlohmann::json NodeEngineMetrics::toJson() const {
    nlohmann::json metricsJson{};
    metricsJson["NODE_ID"] = nodeId;
    metricsJson["PROCESSED_TASKS"] = processedTasks;
    metricsJson["PROCESSED_TUPLE"] = processedTuple;
    metricsJson["PROCESSED_BUFFERS"] = processedBuffers;
    metricsJson["PROCESSED_WATERMARKS"] = processedWatermarks;
    metricsJson["LATENCY_SUM"] = latencySum;
    metricsJson["QUEUE_SIZE_SUM"] = queueSizeSum;
    metricsJson["AVAILABLE_GLOBAL_BUFFER_SUM"] = availableGlobalBufferSum;
    metricsJson["AVAILABLE_FIXED_BUFFER_SUM"] = availableFixedBufferSum;
    metricsJson["TIMESTAMP_QUERY_START"] = timestampQueryStart;
    metricsJson["TIMESTAMP_FIRST_PROCESSED_TASK"] = timestampFirstProcessedTask;
    metricsJson["TIMESTAMP_LAST_PROCESSED_TASK"] = timestampLastProcessedTask;
    metricsJson["QUERY_ID"] = queryId; 
    metricsJson["SUB_QUERY_ID"] = subQueryId;
    // metricsJson["TS_TO_LATENCY_MAP"] = tsToLatencyMap;  // TODO: should be added when non BasicType can be handled
    return metricsJson;
}

void writeToBuffer(const NodeEngineMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.writeToBuffer(buf, tupleIndex);
}

void readFromBuffer(NodeEngineMetrics& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
    metrics.readFromBuffer(buf, tupleIndex);
}

nlohmann::json asJson(const NodeEngineMetrics& metrics) { return metrics.toJson(); }

}// namespace NES::Monitoring