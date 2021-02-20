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

#ifndef NES_KAFKASOURCEE_HPP
#define NES_KAFKASOURCEE_HPP

#include <cstring>
#include <boost/foreach.hpp>
#include <Sources/DataSource.hpp>
#include <boost/algorithm/string.hpp>
#include <NodeEngine/TupleBuffer.hpp>
#include <NodeEngine/BufferManager.hpp>
#include <KafkaConnectorConfiguration.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>

#define foreach BOOST_FOREACH

namespace NES
{
    using KafkaConsumer    = NES_KAFKA::KafkaManualCommitConsumer;
    using KafkaConsumerPtr = std::unique_ptr<KafkaConsumer>;
    using Partitions       = std::vector<std::set<NES_KAFKA::Partition>>;
    using Topics           = std::vector<NES_KAFKA::Topic>;

    class KafkaSource: public DataSource
    {
        public:

            /**
             * KafkaSource Constructor
             *
             * @param SchemaPtr
             * @param NodeEngine::BufferManagerPtr
             * @param NodeEngine::QueryManagerPtr
             * @param OperatorId
             * @param KafkaConnectorConfiguration&
             * @param const Topics&
             * @param const Partitions&
             */
            KafkaSource(
                SchemaPtr,
                NodeEngine::BufferManagerPtr,
                NodeEngine::QueryManagerPtr,
                OperatorId,
                KafkaConnectorConfiguration&,
                const Topics&,
                const Partitions& = {},
                std::string = ",",
                size_t = 2000
            );

            std::optional<NodeEngine::TupleBuffer> receiveData();

            const std::string toString() const;

            SourceType getType() const;

        protected:
            KafkaConsumerPtr source;
            std::string delimiter;
            size_t pollingTime;
    };

    typedef std::shared_ptr<KafkaSource> KafkaSourcePtr;
}


#endif //NES_KAFKASOURCEE_HPP
