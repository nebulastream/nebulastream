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
#include <boost/foreach.hpp>

#define foreach BOOST_FOREACH

namespace NES
{

    KafkaSource::KafkaSource(
            SchemaPtr schema,
            NodeEngine::BufferManagerPtr bufferManager,
            NodeEngine::QueryManagerPtr queryManager,
            OperatorId operatorID,
            KafkaConnectorConfiguration& conf,
            const Topics& topics,
            const Partitions& partitions
    )
    :DataSource(schema, bufferManager, queryManager, operatorID)
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

            this->source->assign(topicPartitions);
        }
    }


    std::optional<NodeEngine::TupleBuffer> KafkaSource::receiveData()
    {
//        NES_DEBUG("KafkaSource is trying to receive data ...");

        // poll the cluster
        std::chrono::milliseconds duration(2000);
        auto records = this->source->poll(duration);

        // xxx
        NodeEngine::TupleBuffer buffer = this->bufferManager->getBufferBlocking();

//        NES_DEBUG(
//                "KafkaSource received #tuples: "
//                        << records.size()
//                        << ", tupleSize: "
//                        << schema->getSchemaSizeInBytes()
//        );

        ::memcpy(buffer.getBuffer(), &(*records.begin()), records.size());
        return buffer;
    }


    const std::string KafkaSource::toString() const
    {
        return std::string();
    }


    SourceType KafkaSource::getType() const
    {
        return SENSE_SOURCE;
    }
}

