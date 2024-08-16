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
class FixedSizeExecutableDataType;

template<typename ValueType>
using ExecutableDataTypePtr = std::shared_ptr<FixedSizeExecutableDataType<ValueType>>;

template<typename ValueType>
class FixedSizeExecutableDataType : public AbstractDataType {
  public:
    explicit FixedSizeExecutableDataType(const nautilus::val<ValueType>& value, const nautilus::val<bool>& null)
        : AbstractDataType(null), rawValue(value) {}

    static ExecDataType create(nautilus::val<ValueType> value, nautilus::val<bool> null = false) {
        return std::make_shared<FixedSizeExecutableDataType<ValueType>>(value, null);
    }

    template<typename CastedDataType>
    nautilus::val<CastedDataType> valueAsType() const {
        if constexpr (std::is_same_v<CastedDataType, ValueType>) {
            return rawValue;
        } else {
            return static_cast<nautilus::val<CastedDataType>>(rawValue);
        }
    }

    nautilus::val<ValueType> operator()() const { return rawValue; }

    ~FixedSizeExecutableDataType() override = default;

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
using ExecDataInt8 = FixedSizeExecutableDataType<Int8>; using ExecDataInt8Ptr = ExecutableDataTypePtr<Int8>;
using ExecDataInt16 = FixedSizeExecutableDataType<Int16>; using ExecDataInt16Ptr = ExecutableDataTypePtr<Int16>;
using ExecDataInt32 = FixedSizeExecutableDataType<Int32>; using ExecDataInt32Ptr = ExecutableDataTypePtr<Int32>;
using ExecDataInt64 = FixedSizeExecutableDataType<Int64>; using ExecDataInt64Ptr = ExecutableDataTypePtr<Int64>;
using ExecDataUInt8 = FixedSizeExecutableDataType<UInt8>; using ExecDataUInt8Ptr = ExecutableDataTypePtr<UInt8>;
using ExecDataUInt16 = FixedSizeExecutableDataType<UInt16>; using ExecDataUInt16Ptr = ExecutableDataTypePtr<UInt16>;
using ExecDataUInt32 = FixedSizeExecutableDataType<UInt32>; using ExecDataUInt32Ptr = ExecutableDataTypePtr<UInt32>;
using ExecDataUInt64 = FixedSizeExecutableDataType<UInt64>; using ExecDataUInt64Ptr = ExecutableDataTypePtr<UInt64>;
using ExecDataFloat = FixedSizeExecutableDataType<Float>; using ExecDataFloatPtr = ExecutableDataTypePtr<Float>;
using ExecDataDouble = FixedSizeExecutableDataType<Double>; using ExecDataDoublePtr = ExecutableDataTypePtr<Double>;
using ExecDataBoolean = FixedSizeExecutableDataType<Boolean>; using ExecDataBooleanPtr = ExecutableDataTypePtr<Boolean>;

ExecDataType readExecDataTypeFromMemRef(MemRefVal& memRef, const PhysicalTypePtr& type);
void writeFixedExecDataTypeToMemRef(MemRefVal& memRef, const ExecDataType& execDataType);

template<typename CastType>
ExecDataType castTo(const ExecDataType& lhs) {
    if (lhs->instanceOf<ExecDataUInt8>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt8>()->valueAsType<uint8_t>()));
    } else if (lhs->instanceOf<ExecDataUInt16>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt16>()->valueAsType<uint16_t>()));
    } else if (lhs->instanceOf<ExecDataUInt32>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt32>()->valueAsType<uint32_t>()));
    } else if (lhs->instanceOf<ExecDataUInt64>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataUInt64>()->valueAsType<uint64_t>()));
    } else if (lhs->instanceOf<ExecDataInt8>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt8>()->valueAsType<int8_t>()));
    } else if (lhs->instanceOf<ExecDataInt16>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt16>()->valueAsType<int16_t>()));
    } else if (lhs->instanceOf<ExecDataInt32>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt32>()->valueAsType<int32_t>()));
    } else if (lhs->instanceOf<ExecDataInt64>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataInt64>()->valueAsType<int64_t>()));
    } else if (lhs->instanceOf<ExecDataFloat>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataFloat>()->valueAsType<float>()));
    } else if (lhs->instanceOf<ExecDataDouble>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataDouble>()->valueAsType<double>()));
    } else if (lhs->instanceOf<ExecDataBoolean>()) {
        return FixedSizeExecutableDataType<CastType>::create(static_cast<nautilus::val<CastType>>(lhs->as<ExecDataBoolean>()->valueAsType<bool>()));
    } else {
        NES_NOT_IMPLEMENTED();
    }
}

std::pair<ExecDataType, ExecDataType> castToSameType(const ExecDataType& lhs, const ExecDataType& rhs);

}// namespace NES::Nautilus

#endif// NES_NAUTILUS_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
