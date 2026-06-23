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

#include <TracedIntegerInputParser.hpp>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <InputParserRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{
namespace
{
/// fast_float/simdjson SWAR constants (little-endian; both ARM and x86 are LE).
constexpr uint64_t SWAR_HI = 0xF0F0F0F0F0F0F0F0ULL;
constexpr uint64_t SWAR_ADD6 = 0x0606060606060606ULL;
constexpr uint64_t SWAR_EXP = 0x3333333333333333ULL; /// per-byte 0x33 iff the byte is an ASCII digit
constexpr uint64_t SWAR_SUB = 0x3030303030303030ULL;
constexpr uint64_t SWAR_MASK = 0x000000FF000000FFULL;
constexpr uint64_t SWAR_MUL1 = 0x000F424000000064ULL; /// 100 + (1000000 << 32)
constexpr uint64_t SWAR_MUL2 = 0x0000271000000001ULL; /// 1 + (10000 << 32)

/// Emit traced ops parsing [addr, addr+size) as an unsigned magnitude: SWAR eight digits at a time
/// (8-byte load -> validity check -> subtract -> combine), then a scalar tail. SWAR runs only on full
/// 8-digit groups still inside the field (the `>= 8` guard), so it never reads past `size`. Each digit
/// that is out of '0'..'9' increments `bad` (0 == valid). All arithmetic; no proxy call -> inlines.
nautilus::val<uint64_t>
tracedDigitsToU64(const nautilus::val<int8_t*>& addr, const nautilus::val<uint64_t>& size, nautilus::val<uint64_t>& bad)
{
    nautilus::val<uint64_t> acc{0};
    nautilus::val<uint64_t> i{0};
    for (; (size - i) >= nautilus::val<uint64_t>{8}; i = i + nautilus::val<uint64_t>{8})
    {
        const nautilus::val<uint64_t> raw = readValueFromMemRef<uint64_t>(addr + i);
        /// simdjson is_made_of_eight_digits_fast: every byte is a digit iff this folds to 0x33 per byte.
        const nautilus::val<uint64_t> chk = (raw & nautilus::val<uint64_t>{SWAR_HI})
            | (((raw + nautilus::val<uint64_t>{SWAR_ADD6}) & nautilus::val<uint64_t>{SWAR_HI}) >> nautilus::val<uint64_t>{4});
        bad = bad + nautilus::val<uint64_t>(chk != nautilus::val<uint64_t>{SWAR_EXP});
        nautilus::val<uint64_t> v = raw - nautilus::val<uint64_t>{SWAR_SUB};
        v = (v * nautilus::val<uint64_t>{10}) + (v >> nautilus::val<uint64_t>{8});
        v = (((v & nautilus::val<uint64_t>{SWAR_MASK}) * nautilus::val<uint64_t>{SWAR_MUL1})
             + (((v >> nautilus::val<uint64_t>{16}) & nautilus::val<uint64_t>{SWAR_MASK}) * nautilus::val<uint64_t>{SWAR_MUL2}))
            >> nautilus::val<uint64_t>{32};
        acc = acc * nautilus::val<uint64_t>{100000000ULL} + v;
    }
    for (; i < size; i = i + nautilus::val<uint64_t>{1})
    {
        const nautilus::val<int8_t> ch = *(addr + i);
        const nautilus::val<uint64_t> d = nautilus::val<uint64_t>(ch) - nautilus::val<uint64_t>{'0'};
        bad = bad + nautilus::val<uint64_t>(d > nautilus::val<uint64_t>{9});
        acc = acc * nautilus::val<uint64_t>{10} + d;
    }
    return acc;
}

/// Wrap a parsed value as a VarVal. Non-nullable -> lean path (the `bad` validity is unused, so the
/// digit-check ops dead-code-eliminate; a malformed field is parsed best-effort -- traced code cannot
/// throw). Nullable -> bad != 0 (non-digit / overflow) becomes isNull (configured null-MARKERS are not
/// matched here; use Default/Fast if you need them).
template <typename T>
VarVal finish(bool nullable, const nautilus::val<T>& value, const nautilus::val<uint64_t>& bad)
{
    if (nullable)
    {
        return VarVal{value, nullable, bad != nautilus::val<uint64_t>{0}};
    }
    return VarVal{value, nullable, false};
}

template <typename T>
VarVal tracedUnsigned(bool nullable, const nautilus::val<int8_t*>& addr, const nautilus::val<uint64_t>& size)
{
    nautilus::val<uint64_t> bad{0};
    bad = bad + nautilus::val<uint64_t>(size == nautilus::val<uint64_t>{0}); /// empty field is invalid
    const nautilus::val<uint64_t> mag = tracedDigitsToU64(addr, size, bad);
    if constexpr (sizeof(T) < sizeof(uint64_t))
    {
        bad = bad + nautilus::val<uint64_t>(mag > nautilus::val<uint64_t>{static_cast<uint64_t>(std::numeric_limits<T>::max())});
    }
    return finish<T>(nullable, nautilus::val<T>(mag), bad);
}

template <typename T>
VarVal tracedSigned(bool nullable, const nautilus::val<int8_t*>& addr, const nautilus::val<uint64_t>& size)
{
    nautilus::val<uint64_t> bad{0};
    bad = bad + nautilus::val<uint64_t>(size == nautilus::val<uint64_t>{0});
    /// Optional leading sign (field is well-formed -> >= 1 char on the valid path).
    const nautilus::val<int8_t> first = *(addr + nautilus::val<uint64_t>{0});
    const nautilus::val<uint64_t> isNeg = nautilus::val<uint64_t>(first == nautilus::val<int8_t>(static_cast<int8_t>('-')));
    const nautilus::val<uint64_t> isSign = isNeg + nautilus::val<uint64_t>(first == nautilus::val<int8_t>(static_cast<int8_t>('+')));
    const nautilus::val<uint64_t> mag = tracedDigitsToU64(addr + isSign, size - isSign, bad);
    /// res = mag * (1 - 2*isNeg): +mag, or the two's-complement -mag (wraps in uint64, reinterpreted on cast).
    const nautilus::val<uint64_t> res = mag - (nautilus::val<uint64_t>{2} * isNeg * mag);
    return finish<T>(nullable, nautilus::val<T>(res), bad);
}
} /// namespace

VarVal TracedINT8InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedSigned<int8_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedINT16InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedSigned<int16_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedINT32InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedSigned<int32_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedINT64InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedSigned<int64_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedUINT8InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedUnsigned<uint8_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedUINT16InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedUnsigned<uint16_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedUINT32InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedUnsigned<uint32_t>(nullable, fieldAddress, fieldSize);
}

VarVal TracedUINT64InputParser::parseToVarVal(
    bool nullable, const nautilus::val<int8_t*>& fieldAddress, const nautilus::val<uint64_t>& fieldSize, const std::vector<std::string>&) const
{
    return tracedUnsigned<uint64_t>(nullable, fieldAddress, fieldSize);
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedINT64InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedUINT8InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedUINT8InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedUINT16InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedUINT16InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedUINT32InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedUINT32InputParser>();
}

InputParserRegistryReturnType InputParserGeneratedRegistrar::RegisterTracedUINT64InputParser(InputParserRegistryArguments)
{
    return std::make_unique<TracedUINT64InputParser>();
}
}
