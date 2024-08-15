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

#ifndef NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
#define NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
#include <Nautilus/DataTypes/AbstractDataType.hpp>
#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <google/protobuf/stubs/port.h>
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

    static ExecDataType create(nautilus::val<ValueType> value, nautilus::val<bool> null = false) {
        return std::make_shared<ExecutableDataType<ValueType>>(value, null);
    }

    template<typename CastedDataType>
    bool isType() {
        return std::is_same_v<ValueType, CastedDataType>;
    }

    template<typename CastedDataType>
    nautilus::val<CastedDataType> valueAsType() const {
        return static_cast<nautilus::val<CastedDataType>>(rawValue);
    }

    nautilus::val<ValueType> operator()() const { return rawValue; }

    /// We should get rid of this call here
    const nautilus::val<ValueType>& getRawValue() const { return rawValue; }

    ~ExecutableDataType() override = default;

  protected:
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

    [[nodiscard]] std::string toString() const override {
        if (null) {
            return "NULL";
        } else {
            return rawValue.toString();
        }
    }

    nautilus::val<bool> isBool() const override { return rawValue == true; }

    nautilus::val<ValueType> rawValue;
};

// Define common ExecutableDataTypes and their pointer counterparts
using ExecDataInt8 = ExecutableDataType<Int8>; using ExecDataInt8Ptr = ExecutableDataTypePtr<Int8>;
using ExecDataInt16 = ExecutableDataType<Int16>; using ExecDataInt16Ptr = ExecutableDataTypePtr<Int16>;
using ExecDataInt32 = ExecutableDataType<Int32>; using ExecDataInt32Ptr = ExecutableDataTypePtr<Int32>;
using ExecDataInt64 = ExecutableDataType<Int64>; using ExecDataInt64Ptr = ExecutableDataTypePtr<Int64>;
using ExecDataUInt8 = ExecutableDataType<UInt8>; using ExecDataUInt8Ptr = ExecutableDataTypePtr<UInt8>;
using ExecDataUInt16 = ExecutableDataType<UInt16>; using ExecDataUInt16Ptr = ExecutableDataTypePtr<UInt16>;
using ExecDataUInt32 = ExecutableDataType<UInt32>; using ExecDataUInt32Ptr = ExecutableDataTypePtr<UInt32>;
using ExecDataUInt64 = ExecutableDataType<UInt64>; using ExecDataUInt64Ptr = ExecutableDataTypePtr<UInt64>;
using ExecDataFloat = ExecutableDataType<Float>; using ExecDataFloatPtr = ExecutableDataTypePtr<Float>;
using ExecDataDouble = ExecutableDataType<Double>; using ExecDataDoublePtr = ExecutableDataTypePtr<Double>;
using ExecDataBoolean = ExecutableDataType<Boolean>; using ExecDataBooleanPtr = ExecutableDataTypePtr<Boolean>;

ExecDataType readExecDataTypeFromMemRef(MemRefVal& memRef, const PhysicalTypePtr& type);
void writeFixedExecDataTypeToMemRef(MemRefVal& memRef, const ExecDataType& execDataType);



/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
class ExecutableVariableDataType : public AbstractDataType {
  public:
    ExecutableVariableDataType(const MemRefVal& content,
                               const nautilus::val<uint32_t>& size,
                               const nautilus::val<bool>& null)
        : AbstractDataType(null), size(size), content(content) {}

    static ExecDataType create(const MemRefVal& content, const nautilus::val<uint32_t>& size, const bool null = false) {
        return std::make_shared<ExecutableVariableDataType>(content, size, null);
    }

    static ExecDataType create(const MemRefVal& pointerToVarSized, const bool null = false) {
        const auto varSized = readValueFromMemRef(pointerToVarSized, uint32_t);
        return create(pointerToVarSized, varSized, null);
    }

    ~ExecutableVariableDataType() override = default;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const { return size; }
    [[nodiscard]] MemRefVal getContent() const { return content + UInt64Val(sizeof(uint32_t)); }
    [[nodiscard]] MemRefVal getReference() const { return content ; }

  protected:
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

    [[nodiscard]] std::string toString() const override {
        std::ostringstream oss;
        oss << "NOT IMPLEMENTED YET!";
        //        oss << " rawValue: " << rawValue << " null: " << null;
        return oss.str();
    }

    nautilus::val<bool> isBool() const override {
        return size > 0 && content != nullptr;
    }

    nautilus::val<uint32_t> size;
    MemRefVal content;
};

template<typename CastType>
ExecDataType castTo(const ExecDataType& lhs) {
    if (lhs->instanceOf<ExecDataUInt8>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt8>()->valueAsType<uint8_t>()));
    } else if (lhs->instanceOf<ExecDataUInt16>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt16>()->valueAsType<uint16_t>()));
    } else if (lhs->instanceOf<ExecDataUInt32>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt32>()->valueAsType<uint32_t>()));
    } else if (lhs->instanceOf<ExecDataUInt64>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt64>()->valueAsType<uint64_t>()));
    } else if (lhs->instanceOf<ExecDataInt8>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt8>()->valueAsType<int8_t>()));
    } else if (lhs->instanceOf<ExecDataInt16>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt16>()->valueAsType<int16_t>()));
    } else if (lhs->instanceOf<ExecDataInt32>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt32>()->valueAsType<int32_t>()));
    } else if (lhs->instanceOf<ExecDataInt64>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt64>()->valueAsType<int64_t>()));
    } else if (lhs->instanceOf<ExecDataFloat>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataFloat>()->valueAsType<float>()));
    } else if (lhs->instanceOf<ExecDataDouble>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataDouble>()->valueAsType<double>()));
    } else if (lhs->instanceOf<ExecDataBoolean>()) {
        return ExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataBoolean>()->valueAsType<bool>()));
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::pair<ExecDataType, ExecDataType> castToSameType(const ExecDataType& lhs, const ExecDataType& rhs);

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
