#include "Identifiers/Identifier.hpp"
#include <ranges>


#include <gtest/gtest.h>
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
namespace NES{

TEST(IdentifierTest, ParseIdentifierList)
{
    const auto *const input = "schema.\"relation\".attribute";
    auto parsed = IdentifierList::parse(input);

    ASSERT_EQ(*std::ranges::begin(parsed), Identifier("schema", false));
    ASSERT_EQ(*(std::ranges::begin(parsed) + 1), Identifier("relation", true));
    ASSERT_EQ(*(std::ranges::begin(parsed) + 2), Identifier("attribute", false));

    ASSERT_EQ(parsed.toString(), "SCHEMA.\"relation\".ATTRIBUTE");
}
}
