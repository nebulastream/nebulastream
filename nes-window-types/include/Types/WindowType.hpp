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
#include <API/Schema.hpp>

#include <memory>
#include <vector>

namespace NES::Windowing
{
class WindowType : public std::enable_shared_from_this<WindowType>
{
public:
    explicit WindowType();

    virtual ~WindowType() = default;

    virtual std::string toString() const = 0;

    virtual bool equal(std::shared_ptr<WindowType> otherWindowType) = 0;

    virtual bool inferStamp(const Schema& schema) = 0;

    /**
     * @brief Get the hash of the window type
     *
     * This function computes a hash value uniquely identifying the window type including characteristic attributes.
     * The hash value is different for different WindowTypes and same WindowTypes with different attributes.
     * Especially a SlidingWindow of same size and slide returns a different hash value,
     * than a TumblingWindow with the same size.
     *
     * @return the hash of the window type
     */
    virtual uint64_t hash() const = 0;
};

}
