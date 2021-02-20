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

#include <Sources/KafkaSourcee.hpp>


namespace NES
{
    KafkaSource::KafkaSource
    (
        SchemaPtr schema,
        NodeEngine::BufferManagerPtr bufferManager,
        NodeEngine::QueryManagerPtr queryManager,
        OperatorId operatorID,
        KafkaConnectorConfiguration &conf,
        const Topics &topics,
        const Partitions &partitions,
        std::string delimiter,
        size_t pollingTime
    )
    : DataSource(schema, bufferManager, queryManager, operatorID), delimiter(delimiter), pollingTime(pollingTime)
    {
        this->source = std::make_unique<KafkaConsumer>(conf.getConfiguration());

        if (partitions.empty())
        {
            // if paritiones are empty then read from all of them
            this->source->subscribe(std::set<NES_KAFKA::Topic>(topics.begin(), topics.end()));
        }
        else
        {

            auto iterator = partitions.begin();
            NES_KAFKA::TopicPartitions topicPartitions;

            foreach(auto topic, topics)
            {
                foreach(auto partition, *iterator)
                {
                    topicPartitions.insert(std::pair<NES_KAFKA::Topic, NES_KAFKA::Partition>(topic, partition));
                }

                ++iterator;
            }

            // assign topic - parition pair to the consumer
            this->source->assign(topicPartitions);
        }
    }


    std::optional<NodeEngine::TupleBuffer> KafkaSource::receiveData()
    {
        std::vector<PhysicalTypePtr> physicalTypes;
        DefaultPhysicalTypeFactory defaultPhysicalTypeFactory = DefaultPhysicalTypeFactory();

        NES_DEBUG("KafkaSource is receiving data ...");

        // poll the cluster
        std::chrono::milliseconds duration(this->pollingTime);
        auto kafkaRecords = this->source->poll(duration);

        NES_DEBUG(
        "KafkaSource received data: "
                << kafkaRecords.size()
                << ", tupleSize: "
                << schema->getSchemaSizeInBytes()
        );

        // fill the buffer
        uint64_t tupleCount = 0;
        uint64_t tupleSize = schema->getSchemaSizeInBytes();
        auto buffer = this->bufferManager->getBufferBlocking();

        for (auto field : schema->fields)
        {
            auto physicalField = defaultPhysicalTypeFactory.getPhysicalType(field->getDataType());
            physicalTypes.push_back(physicalField);
        }

        for (auto& record : kafkaRecords)
        {
            NES_DEBUG("KafkaSource received: tuple=" << tupleCount << " value=" << record.value().toString());

            uint64_t offset = 0;
            std::vector<std::string> tokens;
            // FIXME: Delimiter "," !!!
            boost::algorithm::split(tokens, record.value().toString(), boost::is_any_of(","));

            for (uint64_t j = 0; j < schema->getSize(); j++)
            {
                auto field = physicalTypes[j];
                uint64_t fieldSize = field->size();

                if (field->isBasicType())
                {
                    auto basicPhysicalField = std::dynamic_pointer_cast<BasicPhysicalType>(field);

                    if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_64)
                    {
                        uint64_t val = std::stoull(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_64)
                    {
                        int64_t val = std::stoll(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_32)
                    {
                        uint32_t val = std::stoul(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_32)
                    {
                        int32_t val = std::stol(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16)
                    {
                        uint16_t val = std::stol(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_16)
                    {
                        int16_t val = std::stol(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_16)
                    {
                        uint8_t val = std::stoi(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::INT_8)
                    {
                        int8_t val = std::stoi(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::UINT_8)
                    {
                        int8_t val = std::stoi(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::DOUBLE) {
                        double val = std::stod(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::FLOAT)
                    {
                        float val = std::stof(tokens[j].c_str());
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                    else if (basicPhysicalField->getNativeType() == BasicPhysicalType::BOOLEAN)
                    {
                        bool val = (strcasecmp(tokens[j].c_str(), "true") == 0 || atoi(tokens[j].c_str()) != 0);
                        memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, &val, fieldSize);
                    }
                }
                else
                {
                    memcpy(buffer.getBufferAs<char>() + offset + tupleCount * tupleSize, tokens[j].c_str(), fieldSize);
                }

                offset += fieldSize;
            }

            tupleCount++;
        }

        buffer.setNumberOfTuples(tupleCount);

        return buffer;
    }


    const std::string KafkaSource::toString() const
    {
        return std::string();
    }


    SourceType KafkaSource::getType() const
    {
        return KAFKA_SOURCE;
    }
}

