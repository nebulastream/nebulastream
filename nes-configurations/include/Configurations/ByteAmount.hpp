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

#include <cstdint>
#include <expected>
#include <ostream>
#include <string_view>
#include <fmt/base.h>
#include <fmt/ostream.h>
#include <yaml-cpp/node/convert.h>
#include <yaml-cpp/node/node.h>
#include <ErrorHandling.hpp>

namespace NES
{

/// Parses a byte amount following the Kubernetes resource.Quantity suffix grammar
/// (https://kubernetes.io/docs/reference/kubernetes-api/common-definitions/quantity/):
/// - binary suffixes Ki, Mi, Gi, Ti, Pi, Ei scale by powers of 1024
/// - decimal suffixes k, M, G, T, P, E scale by powers of 1000
/// - no suffix means bytes
/// - suffix matching is case-insensitive, an optional trailing 'B' is ignored: "4KiB" == "4Ki" == "4kib", "4kb" == "4k"
/// - fractional values must scale to a whole number of bytes: "1.5Gi" == 1610612736, "1.5" is rejected
/// Rejects signed values, non-integral results, results exceeding UINT64_MAX, empty input, bare or unknown suffixes,
/// and more than 20 significant fractional digits.
std::expected<uint64_t, Exception> parseByteAmount(std::string_view amount);

/// A number of bytes. As the value type of ScalarOption<Byte>, it is parsed with parseByteAmount.
struct Byte
{
    constexpr Byte() = default;

    constexpr explicit Byte(const uint64_t bytes) : bytes(bytes) { }

    /// Intentionally implicit: a Byte is used like a plain uint64_t byte count.
    constexpr operator uint64_t() const { return bytes; } /// NOLINT(google-explicit-constructor,hicpp-explicit-conversions)

    friend std::ostream& operator<<(std::ostream& os, const Byte& byte) { return os << byte.bytes; }

    uint64_t bytes{0};
};

}

namespace fmt
{
template <>
struct formatter<NES::Byte> : ostream_formatter
{
};
}

namespace YAML
{
/// Parses YAML scalars (`node.as<NES::Byte>()`) with parseByteAmount.
template <>
struct convert<NES::Byte>
{
    static Node encode(const NES::Byte& byte) { return Node(byte.bytes); }

    static bool decode(const Node& node, NES::Byte& result)
    {
        if (!node.IsScalar())
        {
            return false;
        }
        const auto parsed = NES::parseByteAmount(node.Scalar());
        if (!parsed.has_value())
        {
            return false;
        }
        result = NES::Byte(parsed.value());
        return true;
    }
};
}
