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

#ifndef NES_WATERMARKSTRATEGY_HPP
#define NES_WATERMARKSTRATEGY_HPP

#include <Windowing/WindowingForwardRefs.hpp>

namespace NES::Windowing {

class WatermarkStrategy : public std::enable_shared_from_this<WatermarkStrategy> {
  public:
    WatermarkStrategy();

    enum Type {
        EventTimeWatermark,
        ProcessingTimeWatermark,
    };

    virtual Type getType() = 0;

    template<class Type>
    auto as() {
        return std::dynamic_pointer_cast<Type>(shared_from_this());
    }
};
}// namespace NES::Windowing
#endif//NES_WATERMARKSTRATEGY_HPP
