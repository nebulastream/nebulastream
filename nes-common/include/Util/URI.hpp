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

#include <optional>
#include <ostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <Util/Logger/Formatter.hpp>
#include <boost/url/parse.hpp>
#include <boost/url/url.hpp>
#include <yaml-cpp/node/convert.h>
#include <yaml-cpp/node/node.h>

namespace NES
{

/// A strong type wrapper for URI resources based on boost url for validation.
/// Includes additional functionality so that it can be used in ScalarOption
class URI
{
public:
    URI() noexcept = default;

    explicit URI(const std::string_view input)
    {
        auto result = boost::urls::parse_uri(input);
        if (!result)
        {
            throw std::invalid_argument(result.error().message());
        }
        uri = boost::urls::url(*result);
    }

    static std::optional<URI> tryParse(const std::string_view input) noexcept
    {
        if (!boost::urls::parse_uri(input))
        {
            return std::nullopt;
        }
        return URI(input);
    }

    [[nodiscard]] std::string toString() const { return std::string(uri.buffer()); }

    [[nodiscard]] std::string_view view() const noexcept { return uri.buffer(); }

    [[nodiscard]] explicit operator boost::urls::url() const { return uri; }

    friend std::ostream& operator<<(std::ostream& os, const URI& uri) { return os << uri.view(); }

    [[nodiscard]] bool empty() const noexcept { return uri.empty(); }

private:
    boost::urls::url uri;
};

}

template <>
struct YAML::convert<NES::URI>
{
    static ::YAML::Node encode(NES::URI const& rhs)
    {
        ::YAML::Node node;
        node = rhs.toString();
        return node;
    }

    static bool decode(const ::YAML::Node& node, NES::URI& rhs)
    {
        if (!node.IsScalar())
        {
            return false;
        }
        rhs = NES::URI{node.as<std::string>()};
        return true;
    }
};

namespace fmt
{
FMT_OSTREAM(NES::URI);
}
