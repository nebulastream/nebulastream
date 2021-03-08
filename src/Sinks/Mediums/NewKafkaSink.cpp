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

#include <Sinks/Mediums/NewKafkaSink.hpp>
#include <fstream>



namespace NES {
/**
 * TODO:comment
 *
 * @param sinkFormat
 * @param parentPlanId
 * @param topics
 * @param partitions
 */
    KafkaSink::KafkaSink(
            NES::SinkFormatPtr sinkFormat,
            QuerySubPlanId parentPlanId,
            KafkaConnectorConfiguration& conf,
            Topics topics,
            Partitions partitions
    ) : SinkMedium(sinkFormat, parentPlanId), _topics(topics), _partitions(partitions)
    {
        this->producer = std::make_unique<KafkaProducer>(conf.getConfiguration());
    }


/**
 * TODO: comment
 *
 * @return
 */

    bool KafkaSink::writeData(NodeEngine::TupleBuffer& inputBuffer, NodeEngine::WorkerContext&)
    {
       // NES_ASSERT2(_topics.size() == 1, "Currently we support only 1 topic but provided "<< _topics.size());
        // there is space for improvement, you can specify multiple topics, each with its own specific partitions -> simple change of the logic
        std::cout << "Topics: " << _topics.size()<<std::endl;
        std::cout << "Partitions: " << _partitions[0].size()<<std::endl; // give me number of partitions specified for the only topic!!!
        auto numberOfPArtitions = 0;
        if(_partitions.size()!=0)
       {
           numberOfPArtitions = _partitions[0].size();
       }
        
        int round_robin = 0;
        auto numberOfTuples = inputBuffer.getNumberOfTuples();
        auto buffer = inputBuffer.getBufferAs<char>();
        auto physicalDataTypeFactory = DefaultPhysicalTypeFactory();

        // no specific partitions, use the constructor only with topic, it will automatically divide the data  between them
       if(_partitions.size()==0)
        {
            for(uint64_t i = 0; i < numberOfTuples; i++) {
                std::stringstream ss;
                uint64_t offset = 0;
                // the schema is previously created and given as argument to the createKafkaSink =  SinkFormat
                SchemaPtr schema = sinkFormat->getSchemaPtr();
                for (uint64_t j = 0; j < schema->getSize(); j++) {
                    auto field = schema->get(j);
                    auto ptr = field->getDataType();
                    auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
                    auto fieldSize = physicalType->size();
                    auto str = physicalType->convertRawToString(buffer + offset + i *  schema->getSchemaSizeInBytes());
                    ss << str.c_str();
                    if (j < schema->getSize() - 1) {
                        ss << ",";
                    }
                    offset += fieldSize;
                }
                std::cout << "Writing line: " << ss.str();
                for (int k = 0; k < _topics.size(); k++) {
                    // writing the same data to all of the topics!!!
                    auto record =
                            NES_KAFKA::ProducerRecord(_topics[k], NES_KAFKA::Key(), NES_KAFKA::Value(ss.str().c_str(), ss.str().size()));
                    this->producer->send(record);
                }

            }

        }
        else
        {
            for(uint64_t i = 0; i < numberOfTuples; i++) {
                uint64_t offset = 0;
                std::stringstream ss;
                // the schema is previously created and given as argument to the createKafkaSink =  SinkFormat
                SchemaPtr schema = sinkFormat->getSchemaPtr();
                for (uint64_t j = 0; j < schema->getSize(); j++) {
                    auto field = schema->get(j);
                    auto ptr = field->getDataType();
                    auto physicalType = physicalDataTypeFactory.getPhysicalType(ptr);
                    auto fieldSize = physicalType->size();
                    auto str = physicalType->convertRawToString(buffer + offset + i * schema->getSchemaSizeInBytes());
                    ss << str.c_str();
                    if (j < schema->getSize() - 1) {
                        ss << ",";
                    }
                    offset += fieldSize;
                }
                ss << std::endl;
                std::cout << "Writing line: " << ss.str().c_str();
                auto iterator = _partitions.begin();// {{1,2},{0}}, first take {1,2} for _topics[0]
                auto partitions_for_first_topic = _partitions[0];

                for (int k = 0; k < _topics.size(); k++) {
                    std::cout << "Topic " << _topics[k]<< std::endl;
                    std::cout << "Partition number: " << iterator->size()<< std::endl;
                    if(iterator->size()==1)
                    {
                        // always write in this partition for the specific topic
                        foreach (auto partition, *iterator) {
                                std::cout << "1Partition " << partition << std::endl;
                                        auto record = NES_KAFKA::ProducerRecord(_topics[k],
                                                                                partition,
                                                                                NES_KAFKA::Key(),
                                                                                NES_KAFKA::Value(ss.str().c_str(), ss.str().size()));
                                        this->producer->send(record);
                            }

                    }
                    else // there are more partitions for a specific topic
                    {

//                        foreach (auto partition, *iterator) {
                                std::cout<<"Partition " << partitions_for_first_topic[round_robin%numberOfPArtitions]<< std::endl;

                                  auto record = NES_KAFKA::ProducerRecord(_topics[k],
                                                                         partitions_for_first_topic[round_robin%numberOfPArtitions],
                                                                          NES_KAFKA::Key(),
                                                                        NES_KAFKA::Value(ss.str().c_str(), ss.str().size()));
                                this->producer->send(record);
//                          }
                    }
                    round_robin++;
                    ++iterator; // next set of partitions
                }

            }

        }
        return true;



    }

    void KafkaSink::shutdown() {}

    void KafkaSink::setup() {}

    std::string KafkaSink::toString() { return "KAFKA_SINK"; }
    const std::string KafkaSink::toString() const {
        std::stringstream ss;
        ss << "KafkaSink(";
        ss << "SCHEMA(" << sinkFormat->getSchemaPtr()->toString() << "), ";
        return ss.str();
    }

    SinkMediumTypes KafkaSink::getSinkMediumType() { return KAFKA_SINK; }

};
