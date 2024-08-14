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
#include <Common/PhysicalTypes/BasicPhysicalType.hpp>
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

// TODO define here the C++ data types and the nautilus data types. this way, we can use the c++ data types in the proxy functions
// TODO e.g.
// TODO using ExecMemRef = nautilus::val<int8_t*>;
// TODO using MemRef = int8_t*;
// TODO we do not want to use voidref as we can simply wirte val<class_name*> and val<struct_name*> instead
// TODO this should be automatically translated to clas_name* and struct_name* in the proxy functions after calling invoke


/// Define common nautilus data types, we might move this definition some place else
using VoidRef = nautilus::val<void*>;
using MemRef = nautilus::val<int8_t*>;
using Int8 = nautilus::val<int8_t>;
using Int16 = nautilus::val<int16_t>;
using Int32 = nautilus::val<int32_t>;
using Int64 = nautilus::val<int64_t>;
using UInt8 = nautilus::val<uint8_t>;
using UInt16 = nautilus::val<uint16_t>;
using UInt32 = nautilus::val<uint32_t>;
using UInt64 = nautilus::val<uint64_t>;
using Float = nautilus::val<float>;
using Double = nautilus::val<double>;
using Boolean = nautilus::val<bool>;


using ExecDataInt8 = ExecutableDataType<int8_t>;
using ExecDataInt16 = ExecutableDataType<int16_t>;
using ExecDataInt32 = ExecutableDataType<int32_t>;
using ExecDataInt64 = ExecutableDataType<int64_t>;
using ExecDataUInt8 = ExecutableDataType<uint8_t>;
using ExecDataUInt16 = ExecutableDataType<uint16_t>;
using ExecDataUInt32 = ExecutableDataType<uint32_t>;
using ExecDataUInt64 = ExecutableDataType<uint64_t>;
using ExecDataFloat = ExecutableDataType<float>;
using ExecDataDouble = ExecutableDataType<double>;
using ExecDataBoolean = ExecutableDataType<bool>;

using ExecDataInt8Ptr = ExecutableDataTypePtr<int8_t>;
using ExecDataInt16Ptr = ExecutableDataTypePtr<int16_t>;
using ExecDataInt32Ptr = ExecutableDataTypePtr<int32_t>;
using ExecDataInt64Ptr = ExecutableDataTypePtr<int64_t>;
using ExecDataUInt8Ptr = ExecutableDataTypePtr<uint8_t>;
using ExecDataUInt16Ptr = ExecutableDataTypePtr<uint16_t>;
using ExecDataUInt32Ptr = ExecutableDataTypePtr<uint32_t>;
using ExecDataUInt64Ptr = ExecutableDataTypePtr<uint64_t>;
using ExecDataFloatPtr = ExecutableDataTypePtr<float>;
using ExecDataDoublePtr = ExecutableDataTypePtr<double>;
using ExecDataBooleanPtr = ExecutableDataTypePtr<bool>;

/**
 * @brief Get member returns the MemRef to a specific class member as an offset to a objectReference.
 * @note This assumes the offsetof works for the classType.
 * @param objectReference reference to the object that contains the member.
 * @param classType type of a class or struct
 * @param member a member that is part of the classType
 */
#define getMember(objectReference, classType, member, dataType) \
    (static_cast<nautilus::val<dataType>>(*static_cast<nautilus::val<dataType*>>(objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member))))))
#define getMemberAsExecDataType(objectReference, classType, member, dataType) \
    (std::dynamic_pointer_cast<ExecutableDataType<dataType>>(ExecutableDataType<dataType>::create(*static_cast<nautilus::val<dataType*>>(objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member)))))))
#define getMemberAsPointer(objectReference, classType, member, dataType) \
    (static_cast<nautilus::val<dataType*>>(objectReference + nautilus::val<uint64_t>((__builtin_offsetof(classType, member)))))

#define writeValueToMemRef(memRef, value, dataType) \
    (*static_cast<nautilus::val<dataType*>>(memRef) = value)
#define readValueFromMemRef(memRef, dataType) \
    (static_cast<nautilus::val<dataType>>(*static_cast<nautilus::val<dataType*>>(memRef)))

// Might move these methods into a MemRefUtils or something like that
ExecDataType readExecDataTypeFromMemRef(MemRef& memRef, const PhysicalTypePtr& type);
void writeExecDataTypeToMemRef(MemRef& memRef, const ExecDataType& execDataType);
Boolean memEquals(MemRef ptr1, MemRef ptr2, const nautilus::val<uint64_t>& size);
void memCopy(MemRef dest, MemRef src, const nautilus::val<size_t>& size);


/// We assume that the first 4 bytes of a int8_t* to any var sized data contains the length of the var sized data
class ExecutableVariableDataType : public AbstractDataType {
  public:
    ExecutableVariableDataType(const MemRef& content,
                               const nautilus::val<uint32_t>& size,
                               const nautilus::val<bool>& null)
        : AbstractDataType(null), size(size), content(content) {}

    static ExecDataType create(MemRef content, const nautilus::val<uint32_t>& size, bool null = false) {
        return std::make_shared<ExecutableVariableDataType>(content, size, null);
    }

    static ExecDataType create(MemRef pointerToVarSized, bool null = false) {
        const auto varSized = readValueFromMemRef(pointerToVarSized, uint32_t);
        const auto pointerToContent = pointerToVarSized + nautilus::val<uint32_t>(sizeof(uint32_t));
        return create(pointerToContent, varSized, null);
    }

    ~ExecutableVariableDataType() override = default;

    [[nodiscard]] nautilus::val<uint32_t> getSize() const { return size; }
    [[nodiscard]] MemRef getContent() const { return content; }

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
    MemRef content;
};

/// Later, we will refactor this and maybe move this
template<typename T>
concept IntegerTypes = std::same_as<T, int8_t> || std::same_as<T, int16_t> || std::same_as<T, int32_t> || std::same_as<T, uint8_t>
    || std::same_as<T, uint16_t> || std::same_as<T, uint32_t> || std::same_as<T, uint64_t> || std::same_as<T, double>
    || std::same_as<T, float>;

template<typename T>
concept DoubleTypes = std::same_as<T, int8_t> || std::same_as<T, int16_t> || std::same_as<T, int32_t> || std::same_as<T, uint8_t>
    || std::same_as<T, uint16_t> || std::same_as<T, uint32_t> || std::same_as<T, uint64_t> || std::same_as<T, double>
    || std::same_as<T, float>;

template<typename T>
concept FixedDataTypes = IntegerTypes<T> || DoubleTypes<T> || std::same_as<T, bool>;

// TODO Define the operators for the ExecDataType
// TODO IMHO should the below operators adapted to:
// TODO ExecDataType op ExecDataType --> ExecDataType
// TODO ExecDataType op nautilus::val<> --> ExecDataType (as we would loose the null information from the ExecDataType)
// TODO nautilus::val<> op nautilus::val<> --> nautilus::val<>
// TODO nautilus::val<> op fixed_data_type --> nautilus::val<>
// TODO question is what to do if we have different types on the left and right side...

// TODO we should also move them to a cpp file and then declare all possible combinations, as this reduces the compilation time
template<FixedDataTypes RHS>
ExecDataType operator+(const ExecDataType& lhs, const RHS& rhs) {
    return lhs + nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator+(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs + ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator+(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator&&(const ExecDataType& lhs, const RHS& rhs) {
    return lhs && nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator&&(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs && ExecutableDataType<RHS>::create(rhs);
}
ExecDataType operator&&(const ExecDataType& lhs, const ExecDataType& rhs);


template<FixedDataTypes RHS>
ExecDataType operator||(const ExecDataType& lhs, const RHS& rhs) {
    return lhs || nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator||(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs || ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator||(const ExecDataType& lhs, const ExecDataType& rhs);


template<FixedDataTypes RHS>
ExecDataType operator==(const ExecDataType& lhs, const RHS& rhs) {
    return lhs == nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator==(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs == ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator==(const ExecDataType& lhs, const ExecDataType& rhs);


template<FixedDataTypes RHS>
ExecDataType operator!=(const ExecDataType& lhs, const RHS& rhs) {
    return lhs != nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator!=(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return *lhs != ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator!=(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator<(const ExecDataType& lhs, const RHS& rhs) {
    return lhs < nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator<(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs < ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator<(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator>(const ExecDataType& lhs, const RHS& rhs) {
    return lhs > nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator>(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return *lhs > ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator>(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator<=(const ExecDataType& lhs, const RHS& rhs) {
    return lhs <= nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator<=(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return *lhs <= ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator<=(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator>=(const ExecDataType& lhs, const RHS& rhs) {
    return lhs >= nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator>=(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return *lhs >= ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator>=(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator-(const ExecDataType& lhs, const RHS& rhs) {
    return lhs - nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator-(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return *lhs - ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator-(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator*(const ExecDataType& lhs, const RHS& rhs) {
    return lhs * nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator*(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs * ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator*(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator/(const ExecDataType& lhs, const RHS& rhs) {
    return lhs / nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator/(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs / ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator/(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator%(const ExecDataType& lhs, const RHS& rhs) {
    return lhs % nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator%(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs % ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator%(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator&(const ExecDataType& lhs, const RHS& rhs) {
    return lhs & nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator&(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs & ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator&(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator|(const ExecDataType& lhs, const RHS& rhs) {
    return lhs | nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator|(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs | ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator|(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator^(const ExecDataType& lhs, const RHS& rhs) {
    return lhs ^ nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator^(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs ^ ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator^(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator<<(const ExecDataType& lhs, const RHS& rhs) {
    return lhs << nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator<<(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs << ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator<<(const ExecDataType& lhs, const ExecDataType& rhs);

template<FixedDataTypes RHS>
ExecDataType operator>>(const ExecDataType& lhs, const RHS& rhs) {
    return lhs >> nautilus::val<RHS>(rhs);
}

template<FixedDataTypes RHS>
ExecDataType operator>>(const ExecDataType& lhs, const nautilus::val<RHS>& rhs) {
    return lhs >> ExecutableDataType<RHS>::create(rhs);
}

ExecDataType operator>>(const ExecDataType& lhs, const ExecDataType& rhs);

ExecDataType operator!(const ExecDataType& lhs);

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

#endif//NES_NES_NAUTILUS_NEW_INCLUDE_NAUTILUS_DATATYPES_EXECUTABLEDATATYPE_HPP_
