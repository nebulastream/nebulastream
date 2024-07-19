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
#include <nautilus/val_ptr.hpp>
#include <nautilus/std/string.hpp>
#include <nautilus/common/Types.hpp>
#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <ostream>
#include <sstream>
#include <fmt/format.h>

namespace NES::Nautilus {

template<typename ValueType>
class ExecutableDataType : public AbstractDataType {
  public:
    explicit ExecutableDataType(const nautilus::val<ValueType>& value, const nautilus::val<bool>& null)
        : AbstractDataType(null), rawValue(value) {}
    static ExecDataType create(nautilus::val<ValueType> value, bool null = false) {
        return std::make_shared<ExecutableDataType<ValueType>>(value, null);
    }

    // Implementing operations on data types
    ExecDataType operator&&(const AbstractDataType& rightExp) const override;
    ExecDataType operator||(const AbstractDataType& rightExp) const override;
    ExecDataType operator==(const AbstractDataType& rightExp) const override;
    ExecDataType operator!=(const AbstractDataType& rightExp) const override;
    ExecDataType operator<(const AbstractDataType& rightExp) const override;
    ExecDataType operator>(const AbstractDataType& rightExp) const override;
    ExecDataType operator<=(const AbstractDataType& rightExp) const override;
    ExecDataType operator>=(const AbstractDataType& rightExp) const override;
    ExecDataType operator+(const AbstractDataType& rightExp) const override;
    ExecDataType operator-(const AbstractDataType& rightExp) const override;
    ExecDataType operator*(const AbstractDataType& rightExp) const override;
    ExecDataType operator/(const AbstractDataType& rightExp) const override;
    ExecDataType operator%(const AbstractDataType& rightExp) const override;
    ExecDataType operator&(const AbstractDataType& rightExp) const override;
    ExecDataType operator|(const AbstractDataType& rightExp) const override;
    ExecDataType operator^(const AbstractDataType& rightExp) const override;
    ExecDataType operator<<(const AbstractDataType& rightExp) const override;
    ExecDataType operator>>(const AbstractDataType& rightExp) const override;
    ExecDataType operator!() const override;

    nautilus::val<ValueType> read();
    template<typename CastedDataType>
    bool isType() { return std::is_same_v<ValueType, CastedDataType>; }

    template<typename CastedDataType>
    nautilus::val<CastedDataType> as() { return static_cast<nautilus::val<CastedDataType>>(rawValue); }

    ~ExecutableDataType() override = default;

  protected:
    [[nodiscard]] std::string toString() const override {
        std::ostringstream oss;
        oss << "NOT IMPLEMENTED YET!";
//        oss << " rawValue: " << rawValue << " null: " << null;
        return oss.str();
    }

  protected:
    nautilus::val<ValueType> rawValue;
};

// Alias for Identifier Values. Otherwise, user need to type Value<Identifier<WorkerId>>
//template<NESIdentifier IdentifierType>
//using ValueId = ExecutableDataType<IdentifierImpl<IdentifierType>>;


// Define common data types
using MemRef = ExecutableDataType<int8_t*>;
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
using Text = ExecutableDataType<std::string>;

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
