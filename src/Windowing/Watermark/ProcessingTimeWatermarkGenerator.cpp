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

#include <Util/Logger.hpp>
#include <Windowing/Watermark/ProcessingTimeWatermarkGenerator.hpp>
#include <chrono>
namespace NES::Windowing {

ProcessingTimeWatermarkGenerator::ProcessingTimeWatermarkGenerator() {}

uint64_t ProcessingTimeWatermarkGenerator::getWatermark() {
    auto ts = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    NES_DEBUG("ProcessingTimeWatermarkGenerator::getWatermark generate ts=" << ts);
    return ts;
}

}// namespace NES::Windowing