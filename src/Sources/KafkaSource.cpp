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

#ifdef ENABLE_KAFKA_BUILD

#include <Sources/KafkaSource.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <NodeEngine/QueryManager.hpp>
#include <Util/Logger.hpp>
#include <cstdint>
#include <cstring>
#include <sstream>
#include <string>

namespace NES {

KafkaSource::KafkaSource(SchemaPtr schema, NodeEngine::BufferManagerPtr bufferManager, NodeEngine::QueryManagerPtr queryManager,
                         const std::string brokers, const std::string topic, const std::string groupId, bool autoCommit,
                         uint64_t kafkaConsumerTimeout, OperatorId operatorId)
    : DataSource(schema, bufferManager, queryManager, operatorId), brokers(brokers), topic(topic), groupId(groupId), autoCommit(autoCommit),
      kafkaConsumerTimeout(std::move(std::chrono::milliseconds(kafkaConsumerTimeout))) {

    config = {{"metadata.broker.list", brokers.c_str()},
              {"group.id", groupId},
              {"enable.auto.commit", autoCommit},
              {"auto.offset.reset", "latest"}};
    _connect();
    NES_INFO("KAFKASOURCE " << this << ": Init KAFKA SOURCE to brokers " << brokers << ", topic " << topic);
}

KafkaSource::~KafkaSource() {}

std::optional<NodeEngine::TupleBuffer> KafkaSource::receiveData() {
    NES_DEBUG("KAFKASOURCE tries to receive data...");

    cppkafka::Message msg = consumer->poll(kafkaConsumerTimeout);

    if (msg) {
        if (msg.get_error()) {
            if (!msg.is_eof()) {
                NES_WARNING("KAFKASOURCE received error notification: " << msg.get_error());
            }
            return std::nullopt;
        } else {
            NodeEngine::TupleBuffer buffer = bufferManager->getBufferBlocking();

            const uint64_t tupleSize = schema->getSchemaSizeInBytes();
            const uint64_t tupleCnt = msg.get_payload().get_size() / tupleSize;

            NES_DEBUG("KAFKASOURCE recv #tups: " << tupleCnt << ", tupleSize: " << tupleSize << ", msg: " << msg.get_payload());

            std::memcpy(buffer.getBuffer(), msg.get_payload().get_data(), msg.get_payload().get_size());
            buffer.setNumberOfTuples(tupleCnt);

            // XXX: maybe commit message every N times
            if (!autoCommit)
                consumer->commit(msg);
            return buffer;
        }
    }

    return std::nullopt;
}

const std::string KafkaSource::toString() const {
    std::stringstream ss;
    ss << "KAFKA_SOURCE(";
    ss << "SCHEMA(" << schema->toString() << "), ";
    ss << "BROKER(" << brokers << "), ";
    ss << "TOPIC(" << topic << "). ";
    return ss.str();
}

void KafkaSource::_connect() {
    consumer = std::make_unique<cppkafka::Consumer>(config);
    // set the assignment callback
    consumer->set_assignment_callback([&](cppkafka::TopicPartitionList& topicPartitions) {
        for (auto&& tpp : topicPartitions) {
            if (tpp.get_offset() == cppkafka::TopicPartition::Offset::OFFSET_INVALID) {
                NES_WARNING("topic " << tpp.get_topic() << ", partition " << tpp.get_partition() << " get invalid offset "
                                     << tpp.get_offset());
                // TODO: enforce to set offset to -1 or abort ?
                auto tuple = consumer->query_offsets(tpp);
                uint64_t high = std::get<1>(tuple);
                tpp.set_offset(high - 1);
            }
        }
        NES_DEBUG("Got assigned " << topicPartitions.size() << " partitions");
    });
    // set the revocation callback
    consumer->set_revocation_callback([&](const cppkafka::TopicPartitionList& topicPartitions) {
        NES_DEBUG(topicPartitions.size() << " partitions revoked");
    });

    consumer->subscribe({topic});
}

SourceType KafkaSource::getType() const { return KAFKA_SOURCE; }

const std::string KafkaSource::getBrokers() const { return brokers; }

const std::string KafkaSource::getTopic() const { return topic; }

const std::string KafkaSource::getGroupId() const { return groupId; }

bool KafkaSource::isAutoCommit() const { return autoCommit; }

const cppkafka::Configuration& KafkaSource::getConfig() const { return config; }

const std::chrono::milliseconds& KafkaSource::getKafkaConsumerTimeout() const { return kafkaConsumerTimeout; }

const std::unique_ptr<cppkafka::Consumer>& KafkaSource::getConsumer() const { return consumer; }

}// namespace NES
#endif