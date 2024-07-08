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

#ifndef NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
#define NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_

#include <nautilus/val.hpp>
#include <ostream>

namespace NES::Nautilus {

class AbstractDataType {
  public:
    virtual ~AbstractDataType() = default;
    friend std::ostream& operator<<(std::ostream& os, const AbstractDataType& type) {
        os << type.toString();
        return os;
    }

  protected:
    [[nodiscard]] virtual std::string toString() const = 0;
};
using DataType = std::shared_ptr<AbstractDataType>;

template<typename ValueType>
class ExecutableDataType : public AbstractDataType {
  public:
    explicit ExecutableDataType(const nautilus::val<ValueType> &value, const nautilus::val<bool> &null) : rawValue(value), null(null) {}

    [[nodiscard]] const nautilus::val<bool> &isNull() const{
        return null;
    }

  protected:
    std::string toString() const override {
        std::ostringstream oss;
        oss << " rawValue: " << rawValue << " null: " << null;
        return oss.str();
    }

  private:
    nautilus::val<ValueType> rawValue;
    const nautilus::val<bool> null;
};

// Define common data types
using Int8 = ExecutableDataType<int8_t>;
using Int16 = ExecutableDataType<int16_t>;
using Int32 = ExecutableDataType<int32_t>;
using Int64 = ExecutableDataType<int64_t>;
using UInt8 = ExecutableDataType<uint8_t>;
using UInt16 = ExecutableDataType<uint16_t>;
using UInt32 = ExecutableDataType<uint32_t>;
using UInt64 = ExecutableDataType<uint64_t>;
using Float = ExecutableDataType<float>;
using Double = ExecutableDataType<double>;
using Boolean = ExecutableDataType<bool>;

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
