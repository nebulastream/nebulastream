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

#ifndef NES_INCLUDE_WATERMARK_WATERMARK_HPP_
#define NES_INCLUDE_WATERMARK_WATERMARK_HPP_

#include <cstdint>
#include <memory>
namespace NES::Windowing {

class Watermark {
  public:
    explicit Watermark();

    /**
     * @brief this function returns the watermark value depending of the type of the inherited class
     * @return watermark value
     */
    virtual uint64_t getWatermark() = 0;
};

typedef std::shared_ptr<Watermark> WatermarkPtr;
}// namespace NES::Windowing
#endif//NES_INCLUDE_WATERMARK_WATERMARK_HPP_
