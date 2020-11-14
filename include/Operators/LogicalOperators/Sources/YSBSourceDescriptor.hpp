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

#ifndef NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_YSBSOURCEDESCRIPTOR_HPP_
#define NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_YSBSOURCEDESCRIPTOR_HPP_

#include <Operators/LogicalOperators/Sources/SourceDescriptor.hpp>

namespace NES {

/**
 * @brief Descriptor defining properties used for creating physical sense source
 */
class YSBSourceDescriptor : public SourceDescriptor {

  public:
    static SourceDescriptorPtr create(size_t numberOfTuplesToProducePerBuffer,
                                      size_t numBuffersToProcess,
                                      size_t frequency,
                                      bool endlessRepeat,
                                      OperatorId operatorId);

    static SourceDescriptorPtr create(std::string streamName,
                                      size_t numberOfTuplesToProducePerBuffer,
                                      size_t numBuffersToProcess,
                                      size_t frequency,
                                      bool endlessRepeat,
                                      OperatorId operatorId);

    bool equal(SourceDescriptorPtr other) override;
    std::string toString() override;

    size_t getNumBuffersToProcess() const;
    size_t getNumberOfTuplesToProducePerBuffer() const;
    size_t getFrequency() const;
    bool isEndlessRepeat() const;

  private:
    explicit YSBSourceDescriptor(size_t numberOfTuplesToProducePerBuffer,
                                 size_t numBuffersToProcess,
                                 size_t frequency,
                                 bool endlessRepeat,
                                 OperatorId operatorId);

    explicit YSBSourceDescriptor(std::string streamName,
                                 size_t numberOfTuplesToProducePerBuffer,
                                 size_t numBuffersToProcess,
                                 size_t frequency,
                                 bool endlessRepeat,
                                 OperatorId operatorId);

  private:
    size_t numBuffersToProcess;
    size_t numberOfTuplesToProducePerBuffer;
    size_t frequency;
    bool endlessRepeat;
};

typedef std::shared_ptr<YSBSourceDescriptor> YSBSourceDescriptorPtr;

}// namespace NES


#endif//NES_INCLUDE_OPERATORS_LOGICALOPERATORS_SOURCES_YSBSOURCEDESCRIPTOR_HPP_
