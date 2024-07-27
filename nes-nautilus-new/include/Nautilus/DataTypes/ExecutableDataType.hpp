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
#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <nautilus/common/Types.hpp>
#include <nautilus/std/string.hpp>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <ostream>
#include <sstream>

namespace NES::Nautilus {

template<typename ValueType>
class ExecutableDataType;

template<typename ValueType>
using ExecutableDataTypePtr = std::shared_ptr<ExecutableDataType<ValueType>>;


template<typename ValueType>
class ExecutableDataType : public AbstractDataType {
  public:
    explicit ExecutableDataType(const nautilus::val<ValueType>& value, const nautilus::val<bool>& null)
        : AbstractDataType(null), rawValue(value) {}
    static ExecutableDataTypePtr<ValueType> create(nautilus::val<ValueType> value,  nautilus::val<bool> null = false) {
        return std::make_shared<ExecutableDataType<ValueType>>(value, null);
    }

    // Implementing operations on data types
    ExecDataType operator&&(const ExecDataType& rightExp) const override;
    ExecDataType operator||(const ExecDataType& rightExp) const override;
    ExecDataType operator==(const ExecDataType& rightExp) const override;
    ExecDataType operator!=(const ExecDataType& rightExp) const override;
    ExecDataType operator<(const ExecDataType& rightExp) const override;
    ExecDataType operator>(const ExecDataType& rightExp) const override;
    ExecDataType operator<=(const ExecDataType& rightExp) const override;
    ExecDataType operator>=(const ExecDataType& rightExp) const override;
    ExecDataType operator+(const ExecDataType& rightExp) const override;
    ExecDataType operator-(const ExecDataType& rightExp) const override;
    ExecDataType operator*(const ExecDataType& rightExp) const override;
    ExecDataType operator/(const ExecDataType& rightExp) const override;
    ExecDataType operator%(const ExecDataType& rightExp) const override;
    ExecDataType operator&(const ExecDataType& rightExp) const override;
    ExecDataType operator|(const ExecDataType& rightExp) const override;
    ExecDataType operator^(const ExecDataType& rightExp) const override;
    ExecDataType operator<<(const ExecDataType& rightExp) const override;
    ExecDataType operator>>(const ExecDataType& rightExp) const override;
    ExecDataType operator!() const override;
    nautilus::val<ValueType> operator()() const { return rawValue; }


    template<typename CastedDataType>
    bool isType() {
        return std::is_same_v<ValueType, CastedDataType>;
    }

    template<typename CastedDataType>
    nautilus::val<CastedDataType> as() const {
        return static_cast<nautilus::val<CastedDataType>>(rawValue);
    }

    const nautilus::val<ValueType>& getRawValue() const { return rawValue; }

    ~ExecutableDataType() override = default;

  protected:
    [[nodiscard]] std::string toString() const override {
        std::ostringstream oss;
        oss << "NOT IMPLEMENTED YET!";
        //        oss << " rawValue: " << rawValue << " null: " << null;
        return oss.str();
    }

    nautilus::val<ValueType> rawValue;
};


// Alias for Identifier Values. Otherwise, user need to type Value<Identifier<WorkerId>>
//template<NESIdentifier IdentifierType>
//using ValueId = ExecutableDataType<IdentifierImpl<IdentifierType>>;

// Define common data types and their pointers
using MemRef = nautilus::val<int8_t*>;
using Int8 = ExecutableDataTypePtr<int8_t>;
using Int16 = ExecutableDataTypePtr<int16_t>;
using Int32 = ExecutableDataTypePtr<int32_t>;
using Int64 = ExecutableDataTypePtr<int64_t>;
using UInt8 = ExecutableDataTypePtr<uint8_t>;
using UInt16 = ExecutableDataTypePtr<uint16_t>;
using UInt32 = ExecutableDataTypePtr<uint32_t>;
using UInt64 = ExecutableDataTypePtr<uint64_t>;
using Float = ExecutableDataTypePtr<float>;
using Double = ExecutableDataTypePtr<double>;
using Boolean = ExecutableDataTypePtr<bool>;




class ExecutableVariableDataType : public AbstractDataType {
  public:
    ExecutableVariableDataType(const nautilus::val<int8_t*>& content,
                               const nautilus::val<uint32_t>& size,
                               const nautilus::val<bool>& null)
        : AbstractDataType(null), size(size), content(content) {}

    static ExecDataType create(nautilus::val<int8_t*> content, const nautilus::val<uint32_t>& size, bool null = false) {
        return std::make_shared<ExecutableVariableDataType>(content, size, null);
    }

    ~ExecutableVariableDataType() override = default;
    ExecDataType operator&&(const ExecDataType&) const override;
    ExecDataType operator||(const ExecDataType&) const override;
    ExecDataType operator==(const ExecDataType&) const override;
    ExecDataType operator!=(const ExecDataType&) const override;
    ExecDataType operator<(const ExecDataType&) const override;
    ExecDataType operator>(const ExecDataType&) const override;
    ExecDataType operator<=(const ExecDataType&) const override;
    ExecDataType operator>=(const ExecDataType&) const override;
    ExecDataType operator+(const ExecDataType&) const override;
    ExecDataType operator-(const ExecDataType&) const override;
    ExecDataType operator*(const ExecDataType&) const override;
    ExecDataType operator/(const ExecDataType&) const override;
    ExecDataType operator%(const ExecDataType&) const override;
    ExecDataType operator&(const ExecDataType&) const override;
    ExecDataType operator|(const ExecDataType&) const override;
    ExecDataType operator^(const ExecDataType&) const override;
    ExecDataType operator<<(const ExecDataType&) const override;
    ExecDataType operator>>(const ExecDataType&) const override;
    ExecDataType operator!() const override;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const { return size; }
    [[nodiscard]] nautilus::val<int8_t*> getContent() const { return content; }

  protected:
    [[nodiscard]] std::string toString() const override {
        std::ostringstream oss;
        oss << "NOT IMPLEMENTED YET!";
        //        oss << " rawValue: " << rawValue << " null: " << null;
        return oss.str();
    }

    nautilus::val<uint32_t> size;
    nautilus::val<int8_t*> content;
};

}// namespace NES::Nautilus

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
