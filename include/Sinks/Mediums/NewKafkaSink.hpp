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

#ifndef NES_NEWKAFKASINK_HPP
#define NES_NEWKAFKASINK_HPP

#include <boost/foreach.hpp>
#include <Sinks/Mediums/SinkMedium.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <fstream>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/PhysicalTypes/PhysicalType.hpp>

#include <KafkaConnectorConfiguration.hpp>
#include <Util/Logger.hpp>

namespace NES {

#define foreach BOOST_FOREACH

    using KafkaProducer = NES_KAFKA::KafkaSyncProducer;
    using KafkaSinkPtr = std::shared_ptr<KafkaProducer>;
//    using KafkaSinkConfiguration = NES_KAFKA::ProducerConfig;
//    using KafkaSinkConfigurationPtr = std::unique_ptr<KafkaSinkConfiguration>;
//    using Partitions = std::vector<std::set<NES_KAFKA::Partition>>;
    using Partitions = std::vector<std::vector<NES_KAFKA::Partition>>;
    using Topics = std::vector<NES_KAFKA::Topic>;

    class KafkaSink : public SinkMedium {

    public:
        /**
         * TODO: comment
         *
         * @return
         */
        explicit KafkaSink(
                SinkFormatPtr,
                QuerySubPlanId,
                KafkaConnectorConfiguration&,
                Topics,
                Partitions = {}
                );

        /**
         * TODO: comment
         *
         * @return
         */
        bool writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContext& workerContext);

        /**
         * TODO: comment
         */
        void shutdown();


        /**
         * TODO: comment
         */
        void setup();


        /**
         * TODO: comment
         *
         * @return
         */
        std::string toString() override;


        /**
         * TODO: comment
         *
         * @return
         */
        const std::string toString() const override;


        /**
         * TODO: comment
         *
         * @return
         */
        SinkMediumTypes getSinkMediumType();


        /**
        * TODO: comment
        *
        * @return
        */
//        static KafkaSinkConfiguration& createConfiguration();

        /**
         * TODO: comment
         *
         * @return
         */
//        static KafkaSinkConfiguration& configuration();

        /**
        * TODO: comment
        *
        * @return
        */
        static std::string confToString();

    protected:
        /**
         * TODO: comment
         */
//        inline static KafkaSinkConfigurationPtr config;

        /**
         * TODO: comment
         */
        KafkaSinkPtr producer;

    private:
        /**
         * TODO: comment
         */
        Topics _topics;

        /**
         * TODO: comment
         */
        Partitions _partitions;
    };

};

#endif//NES_NEWKAFKASINK_HPP