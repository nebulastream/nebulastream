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

#include <string>
#include <yaml-cpp/yaml.h>
#include <boost/url.hpp>

namespace NES
{

struct URI
{
    std::string uri;

    std::string toString() const
    {
        return uri;
    }

    bool empty() const
    {
        return uri.empty();
    }

    void store(const std::string_view inputURI)
    {
        validateURI(inputURI);
        this->uri = inputURI;
    }

    URI() = default;

    explicit URI(const std::string_view inputURI)
    {
        validateURI(inputURI);
        this->uri = inputURI;
    };

private:
    static void validateURI(std::string_view inputURI) {
        auto result = boost::urls::parse_uri(inputURI);
        if (!result)
        {
            throw std::invalid_argument("Invalid URI specified");
        }
    }
};

inline std::ostream& operator<<(std::ostream& os, URI const& u)
{
    return os << u.uri;
}

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
            return false;
        rhs = NES::URI{node.as<std::string>()};
        return true;
    }
};
