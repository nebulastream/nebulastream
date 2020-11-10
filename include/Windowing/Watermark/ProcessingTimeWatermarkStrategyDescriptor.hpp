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

#ifndef NES_PROCESSINGTIMEWATERMARKDESCRIPTOR_HPP
#define NES_PROCESSINGTIMEWATERMARKDESCRIPTOR_HPP

#include <Windowing/Watermark/WatermarkStrategyDescriptor.hpp>

namespace NES::Windowing {

class ProcessingTimeWatermarkStrategyDescriptor;
typedef std::shared_ptr<ProcessingTimeWatermarkStrategyDescriptor> ProcessingTimeWatermarkStrategyDescriptorPtr;

class ProcessingTimeWatermarkStrategyDescriptor : public WatermarkStrategyDescriptor {
  public:
    static WatermarkStrategyDescriptorPtr create();

    bool equal(WatermarkStrategyDescriptorPtr other) override;

  private:
    explicit ProcessingTimeWatermarkStrategyDescriptor();
};

}// namespace NES::Windowing

#endif//NES_PROCESSINGTIMEWATERMARKDESCRIPTOR_HPP
