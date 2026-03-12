#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <QueryId.hpp>

namespace NES
{
namespace detail
{
inline std::optional<uint8_t> decodeHexNibble(const char c)
{
    if (c >= '0' && c <= '9')
    {
        return static_cast<uint8_t>(c - '0');
    }
    if (c >= 'a' && c <= 'f')
    {
        return static_cast<uint8_t>(10 + (c - 'a'));
    }
    if (c >= 'A' && c <= 'F')
    {
        return static_cast<uint8_t>(10 + (c - 'A'));
    }
    return std::nullopt;
}
}

inline std::string hexEncode(std::string_view input)
{
    static constexpr std::string_view digits = "0123456789abcdef";
    std::string encoded;
    encoded.reserve(input.size() * 2);
    for (const auto c : input)
    {
        const auto byte = static_cast<uint8_t>(c);
        encoded.push_back(digits[byte >> 4U]);
        encoded.push_back(digits[byte & 0x0FU]);
    }
    return encoded;
}

inline std::optional<std::string> hexDecode(std::string_view input)
{
    if (input.size() % 2 != 0)
    {
        return std::nullopt;
    }

    std::string decoded;
    decoded.reserve(input.size() / 2);
    for (size_t i = 0; i < input.size(); i += 2)
    {
        const auto high = detail::decodeHexNibble(input[i]);
        const auto low = detail::decodeHexNibble(input[i + 1]);
        if (!high.has_value() || !low.has_value())
        {
            return std::nullopt;
        }
        decoded.push_back(static_cast<char>((*high << 4U) | *low));
    }
    return decoded;
}

inline std::string encodeQueryIdForIdentifier(const QueryId& queryId)
{
    return hexEncode(queryId.getLocalQueryId().getRawValue()) + "-" + hexEncode(queryId.getDistributedQueryId().getRawValue());
}

inline std::optional<QueryId> decodeQueryIdFromIdentifier(std::string_view encodedQueryId)
{
    const auto separator = encodedQueryId.find('-');
    if (separator == std::string_view::npos)
    {
        return std::nullopt;
    }

    const auto localRaw = hexDecode(encodedQueryId.substr(0, separator));
    const auto distributedRaw = hexDecode(encodedQueryId.substr(separator + 1));
    if (!localRaw.has_value() || !distributedRaw.has_value())
    {
        return std::nullopt;
    }

    const auto localQueryId = LocalQueryId(*localRaw);
    const auto distributedQueryId = DistributedQueryId(*distributedRaw);
    const auto hasLocalQueryId = localQueryId != INVALID_LOCAL_QUERY_ID;
    const auto hasDistributedQueryId = distributedQueryId != DistributedQueryId(DistributedQueryId::INVALID);

    if (hasLocalQueryId && hasDistributedQueryId)
    {
        return QueryId::create(localQueryId, distributedQueryId);
    }
    if (hasLocalQueryId)
    {
        return QueryId::createLocal(localQueryId);
    }
    if (hasDistributedQueryId)
    {
        return QueryId::createDistributed(distributedQueryId);
    }
    return std::nullopt;
}
}
