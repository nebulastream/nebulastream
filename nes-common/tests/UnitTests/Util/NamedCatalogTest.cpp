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

#include <Util/NamedCatalog.hpp>

#include <stdexcept>
#include <string>

#include <gtest/gtest.h>

namespace NES
{

namespace
{
/// A trivial descriptor, kept independent of any real catalog's descriptor type (e.g. UdfDescriptor)
/// so this test exercises only the generic container, not any concrete catalog's semantics.
struct TestDescriptor
{
    std::string value;
    bool operator==(const TestDescriptor&) const = default;
};

auto notFound(const std::string& name)
{
    return std::runtime_error("not found: " + name);
}
}

TEST(NamedCatalogTest, RegisterAndHasEntry)
{
    NamedCatalog<TestDescriptor> catalog;
    catalog.registerEntry("a", TestDescriptor{"1"});
    EXPECT_TRUE(catalog.hasEntry("a"));
    EXPECT_FALSE(catalog.hasEntry("b"));
}

TEST(NamedCatalogTest, LoadReturnsRegisteredEntry)
{
    NamedCatalog<TestDescriptor> catalog;
    catalog.registerEntry("a", TestDescriptor{"1"});
    EXPECT_EQ(catalog.load("a", [] { return notFound("a"); }), (TestDescriptor{"1"}));
}

TEST(NamedCatalogTest, LoadUnknownEntryThrowsCallerSuppliedException)
{
    const NamedCatalog<TestDescriptor> catalog;
    EXPECT_THROW((void)catalog.load("missing", [] { return notFound("missing"); }), std::runtime_error);
}

TEST(NamedCatalogTest, RegisterOverwritesExistingEntry)
{
    NamedCatalog<TestDescriptor> catalog;
    catalog.registerEntry("a", TestDescriptor{"1"});
    catalog.registerEntry("a", TestDescriptor{"2"});
    EXPECT_EQ(catalog.load("a", [] { return notFound("a"); }), (TestDescriptor{"2"}));
}

TEST(NamedCatalogTest, RemoveEntry)
{
    NamedCatalog<TestDescriptor> catalog;
    catalog.registerEntry("a", TestDescriptor{"1"});
    ASSERT_TRUE(catalog.hasEntry("a"));
    catalog.removeEntry("a");
    EXPECT_FALSE(catalog.hasEntry("a"));
}

TEST(NamedCatalogTest, GetNamesAndGetEntries)
{
    NamedCatalog<TestDescriptor> catalog;
    catalog.registerEntry("a", TestDescriptor{"1"});
    catalog.registerEntry("b", TestDescriptor{"2"});
    EXPECT_EQ(catalog.getNames().size(), 2U);
    EXPECT_EQ(catalog.getEntries().size(), 2U);
}

}
