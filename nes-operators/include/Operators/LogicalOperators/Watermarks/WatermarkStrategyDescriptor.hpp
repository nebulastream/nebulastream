/*
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

#pragma once

#include <memory>
#include <API/Schema.hpp>

namespace NES::Windowing
{


class WatermarkStrategyDescriptor : public std::enable_shared_from_this<WatermarkStrategyDescriptor>
{
public:
    WatermarkStrategyDescriptor();
    virtual ~WatermarkStrategyDescriptor() = default;
    virtual bool equal(std::shared_ptr<WatermarkStrategyDescriptor> other) = 0;

    virtual std::string toString() = 0;

    virtual bool inferStamp(const std::shared_ptr<Schema>& schema) = 0;
};
}
