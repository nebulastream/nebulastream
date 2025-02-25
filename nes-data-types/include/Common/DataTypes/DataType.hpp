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
#include <string>

namespace NES
{

//// @brief Base data type, which is the parent class for all other data types
class DataType
{
public:
    virtual ~DataType() = default;

    /// @brief Calculates the joined data type between this data type and the other.
    /// If they have no possible joined data type, the coined type is Undefined.
    /// Floats, we can join with all numeric data types.
    virtual std::unique_ptr<DataType> join(const DataType& otherDataType) const = 0;

    template <class T>
    static std::shared_ptr<T> as(const std::shared_ptr<DataType>& ptr)
    {
        return std::dynamic_pointer_cast<T>(ptr);
    }

    virtual std::unique_ptr<DataType> clone() const = 0;
    virtual bool operator==(const DataType& other) const = 0;
    bool operator!=(const DataType& other) const { return !(*this == other); }
    [[nodiscard]] virtual std::string toString() const = 0;
};

}
