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

#include <Functions/PhysicalFunctionPluginMacros.hpp>

#include <bit>
#include <cstdint>
#include <cstring>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <ryu/ryu.h>
#include <std/cstring.h>
#include <uuid/uuid.h>
#include <function.hpp>
#include <val.hpp>
#include "Functions/ConstantValueVariableSizePhysicalFunction.hpp"
#include "Util/Overloaded.hpp"

namespace NES
{

/// Converts two UINT64 values (high bits, low bits) into a UUID string using libuuid.
/// Output format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars + null terminator).
NES_PHYSICAL_FUNCTION(2, Uuid)
{
    const auto highValue = childFunctions[0].execute(record, arena);
    const auto lowValue = childFunctions[1].execute(record, arena);

    const auto isNullable = highValue.isNullable() or lowValue.isNullable();
    if (isNullable and (highValue.isNull() or lowValue.isNull()))
    {
        return VarVal{VariableSizedData{nullptr, 0}, true, true};
    }

    const auto high = highValue.getRawValueAs<nautilus::val<uint64_t>>();
    const auto low = lowValue.getRawValueAs<nautilus::val<uint64_t>>();

    /// 36 chars + null terminator
    constexpr uint32_t uuidStringLen = 37;
    auto uuidData = arena.allocateVariableSizedData(nautilus::val<uint32_t>{uuidStringLen});
    const auto payload = uuidData.getContent();

    nautilus::invoke(
        +[](const uint64_t highBits, const uint64_t lowBits, char* out)
        {
            /// uuid_t is uint8_t[16] in big-endian byte order
            const auto highBE = std::endian::native == std::endian::big ? highBits : std::byteswap(highBits);
            const auto lowBE = std::endian::native == std::endian::big ? lowBits : std::byteswap(lowBits);
            uuid_t uuid;
            std::memcpy(uuid, &highBE, 8);
            std::memcpy(uuid + 8, &lowBE, 8);
            uuid_unparse_lower(uuid, out);
        },
        high,
        low,
        payload);

    return VarVal{uuidData, isNullable, false};
}

#define TO_STRING(Type) \
    class ToString_##Type##_PhysicalFunction final \
    { \
    public: \
        explicit ToString_##Type##_PhysicalFunction(PhysicalFunction valueFunction); \
        [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const; \
\
    private: \
        ConstantValueVariableSizePhysicalFunction trueFn{reinterpret_cast<const int8_t*>("true"), 4}; \
        ConstantValueVariableSizePhysicalFunction falseFn{reinterpret_cast<const int8_t*>("false"), 5}; \
        PhysicalFunction valueFunction; \
    }; \
\
    static_assert(PhysicalFunctionConcept<ToString_##Type##_PhysicalFunction>); \
\
    ToString_##Type##_PhysicalFunction::ToString_##Type##_PhysicalFunction(PhysicalFunction valueFunction) \
        : valueFunction(std::move(valueFunction)) \
    { \
    } \
\
    VarVal ToString_##Type##_PhysicalFunction::execute([[maybe_unused]] const Record& record, [[maybe_unused]] ArenaRef& arena) const
#define INTER_TO_STRING_IMPL(Type) \
    { \
        auto varsized = arena.allocateVariableSizedData(int_str_buf_size<Type>()); \
        auto value = valueFunction.execute(record, arena); \
        nautilus::val<uint32_t> actualSize \
            = nautilus::invoke(int_to_str<Type>, value.getRawValueAs<nautilus::val<Type>>(), varsized.getContent()); \
        return VariableSizedData{ \
            varsized.getContent() + (nautilus::val<uint32_t>(int_str_buf_size<Type>()) - actualSize - 1), actualSize}; \
    }

template <typename T>
constexpr size_t int_str_buf_size()
{
    return sizeof(T) * 3 + 2;
}

template <typename T>
uint32_t int_to_str(T val, char* buf)
{
    static_assert(std::is_integral_v<T>, "int_to_str: T must be an integral type");

    constexpr size_t N = int_str_buf_size<T>();
    char* p = buf + N - 1;
    *p = '\0';

    if (val == 0)
    {
        *--p = '0';
        return 1;
    }

    bool neg = false;
    std::make_unsigned_t<T> uval;
    if constexpr (std::is_signed_v<T>)
    {
        if (val < 0)
        {
            neg = true;
            uval = static_cast<std::make_unsigned_t<T>>(-val);
        }
        else
        {
            uval = static_cast<std::make_unsigned_t<T>>(val);
        }
    }
    else
    {
        uval = val;
    }

    uint32_t size = 0;
    while (uval != 0)
    {
        *--p = '0' + static_cast<char>(uval % 10);
        uval /= 10;
        ++size;
    }
    if (neg)
    {
        *--p = '-';
        ++size;
    }
    return size;
}

TO_STRING(uint8_t) INTER_TO_STRING_IMPL(uint8_t);
TO_STRING(uint16_t) INTER_TO_STRING_IMPL(uint16_t);
TO_STRING(uint32_t) INTER_TO_STRING_IMPL(uint32_t);
TO_STRING(uint64_t) INTER_TO_STRING_IMPL(uint64_t);
TO_STRING(int8_t) INTER_TO_STRING_IMPL(int8_t);
TO_STRING(int16_t) INTER_TO_STRING_IMPL(int16_t);
TO_STRING(int32_t) INTER_TO_STRING_IMPL(int32_t);
TO_STRING(int64_t) INTER_TO_STRING_IMPL(int64_t);

TO_STRING(float)
{
    auto varsized = arena.allocateVariableSizedData(16);
    auto value = valueFunction.execute(record, arena);
    nautilus::val<uint32_t> actualSize = nautilus::invoke(
        +[](float value, char* buf) { return f2s_buffered_n(value, buf); },
        value.getRawValueAs<nautilus::val<float>>(),
        varsized.getContent());
    return VariableSizedData{varsized.getContent(), actualSize};
}

TO_STRING(double)
{
    auto varsized = arena.allocateVariableSizedData(25);
    auto value = valueFunction.execute(record, arena);
    nautilus::val<uint32_t> actualSize = nautilus::invoke(
        +[](double value, char* buf) { return d2s_buffered_n(value, buf); },
        value.getRawValueAs<nautilus::val<double>>(),
        varsized.getContent());
    return VariableSizedData{varsized.getContent(), actualSize};
}

TO_STRING(bool)
{
    auto value = valueFunction.execute(record, arena);
    if (value.getRawValueAs<nautilus::val<bool>>())
    {
        return trueFn.execute(record, arena);
    }
    else
    {
        return falseFn.execute(record, arena);
    }
}

TO_STRING(char)
{
    auto varsized = arena.allocateVariableSizedData(1);
    auto value = valueFunction.execute(record, arena);
    *varsized.getContent() = value.getRawValueAs<nautilus::val<char>>();
    return VariableSizedData{varsized.getContent(), 1};
}

TO_STRING(VarSized)
{
    return valueFunction.execute(record, arena);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTo_StringPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    PRECONDITION(args.childFunctions.size() == (1), "TO_STRING PhysicalFunction must have exactly 1 child function(s)");
    auto child = std::move(args.childFunctions[0]);
    switch (args.inputTypes.at(0).type)
    {
        case DataType::Type::UINT8:
            return PhysicalFunction(ToString_uint8_t_PhysicalFunction(std::move(child)));
        case DataType::Type::UINT16:
            return PhysicalFunction(ToString_uint16_t_PhysicalFunction(std::move(child)));
        case DataType::Type::UINT32:
            return PhysicalFunction(ToString_uint32_t_PhysicalFunction(std::move(child)));
        case DataType::Type::UINT64:
            return PhysicalFunction(ToString_uint64_t_PhysicalFunction(std::move(child)));
        case DataType::Type::INT8:
            return PhysicalFunction(ToString_int8_t_PhysicalFunction(std::move(child)));
        case DataType::Type::INT16:
            return PhysicalFunction(ToString_int16_t_PhysicalFunction(std::move(child)));
        case DataType::Type::INT32:
            return PhysicalFunction(ToString_int32_t_PhysicalFunction(std::move(child)));
        case DataType::Type::INT64:
            return PhysicalFunction(ToString_int64_t_PhysicalFunction(std::move(child)));
        case DataType::Type::FLOAT32:
            return PhysicalFunction(ToString_float_PhysicalFunction(std::move(child)));
        case DataType::Type::FLOAT64:
            return PhysicalFunction(ToString_double_PhysicalFunction(std::move(child)));
        case DataType::Type::BOOLEAN:
            return PhysicalFunction(ToString_bool_PhysicalFunction(std::move(child)));
        case DataType::Type::CHAR:
            return PhysicalFunction(ToString_char_PhysicalFunction(std::move(child)));
        case DataType::Type::VARSIZED:
            return PhysicalFunction(ToString_VarSized_PhysicalFunction(std::move(child)));
        case DataType::Type::UNDEFINED:
            INVARIANT(args.inputTypes.at(0).type != DataType::Type::UNDEFINED, "Undefined should have been resolved");
    }
    std::unreachable();
}

class FMTPhysicalFunction final
{
public:
    explicit FMTPhysicalFunction(
        std::vector<std::variant<std::pair<std::shared_ptr<const std::byte[]>, size_t>, PhysicalFunction>> fragments);
    [[nodiscard]] VarVal execute(const Record& record, ArenaRef& arena) const;

private:
    size_t baseAllocation = 0;
    std::vector<std::variant<std::pair<std::shared_ptr<const std::byte[]>, size_t>, PhysicalFunction>> fragments;
};

static_assert(PhysicalFunctionConcept<FMTPhysicalFunction>);

FMTPhysicalFunction::FMTPhysicalFunction(
    std::vector<std::variant<std::pair<std::shared_ptr<const std::byte[]>, size_t>, PhysicalFunction>> fragments)
    : fragments(std::move(fragments))
{
    for (const auto& fragment : this->fragments)
    {
        if (auto textFragment = std::get_if<std::pair<std::shared_ptr<const std::byte[]>, size_t>>(&fragment))
        {
            baseAllocation += textFragment->second;
        }
    }
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterFMTPhysicalFunction(PhysicalFunctionRegistryArguments args)
{
    auto fmt = args.childFunctions.at(0).tryGetAs<ConstantValueVariableSizePhysicalFunction>();
    std::vector<std::variant<std::pair<std::shared_ptr<const std::byte[]>, size_t>, PhysicalFunction>> fragments;
    const auto& data = fmt->get().getData();

    size_t argumentCounter = 1;
    bool inArgument = false;
    size_t lastFragmentStart = 0;
    for (size_t i = 0; i < data.size(); i++)
    {
        if (!inArgument && data[i] == '{')
        {
            if (i > lastFragmentStart)
            {
                auto fragment = std::make_unique<std::byte[]>(i - lastFragmentStart);
                std::memcpy(fragment.get(), data.data() + lastFragmentStart, i - lastFragmentStart);
                fragments.emplace_back(std::pair{std::move(fragment), i - lastFragmentStart});
            }
            inArgument = true;
        }
        else if (inArgument && data[i] == '}')
        {
            fragments.emplace_back(args.childFunctions[argumentCounter++]);
            lastFragmentStart = i + 1;
            inArgument = false;
        }
    }
    INVARIANT(inArgument == false, "Missing closing bracket in FMT string");
    if (lastFragmentStart < data.size())
    {
        auto fragment = std::make_unique<std::byte[]>(data.size() - lastFragmentStart);
        std::memcpy(fragment.get(), data.data() + lastFragmentStart, data.size() - lastFragmentStart);
        fragments.emplace_back(std::pair{std::move(fragment), data.size() - lastFragmentStart});
    }


    return FMTPhysicalFunction(std::move(fragments));
}

VarVal FMTPhysicalFunction::execute([[maybe_unused]] const Record& record, [[maybe_unused]] ArenaRef& arena) const
{
    nautilus::val<uint32_t> totalSize = nautilus::val<uint32_t>(baseAllocation);
    std::vector<VariableSizedData> args;
    for (const auto& fragment : nautilus::static_iterable(fragments))
    {
        if (auto argument = std::get_if<PhysicalFunction>(&fragment))
        {
            args.emplace_back(argument->execute(record, arena).getRawValueAs<VariableSizedData>());
            totalSize += args.back().getSize();
        }
    }
    auto resultString = arena.allocateVariableSizedData(totalSize);
    nautilus::val<size_t> offset = 0;
    size_t argument = 0;
    for (const auto& fragment : nautilus::static_iterable(fragments))
    {
        if (auto textFragment = std::get_if<std::pair<std::shared_ptr<const std::byte[]>, size_t>>(&fragment))
        {
            nautilus::memcpy(resultString.getContent() + offset, textFragment->first.get(), textFragment->second);
            offset += textFragment->second;
        }
        else if (std::holds_alternative<PhysicalFunction>(fragment))
        {
            nautilus::memcpy(resultString.getContent() + offset, args.at(argument).getContent(), args.at(argument).getSize());
            offset += args[argument].getSize();
            argument++;
        }
    }
    return resultString;
}

}
