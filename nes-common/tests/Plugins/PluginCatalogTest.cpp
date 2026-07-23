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

#include <gtest/gtest.h>

#include <any>
#include <Plugins/PluginDescriptor.hpp>
#include <Plugins/PluginCatalog.hpp>
#include <Plugins/PluginLoader.hpp>
#include <Plugins/RegistryCatalog.hpp>
#include <ErrorHandling.hpp>
#include <TestPluginRegistry.hpp>

namespace NES
{

/// End-to-end test of dynamic plugin loading against a test-only registry: the host (this test
/// binary) links libnes.so and owns the TestPluginRegistry; the plugin (nes-test-plugin MODULE,
/// fully generated) links the same shared core and describes one entry for it via
/// nes_plugin_describe. Host and plugin must observe the same registry instance.
TEST(PluginCatalogTest, LoadPluginAndResolveRegisteredEntry)
{
    /// Constructing the manager loads the built-in plugins, which announces the host-owned
    /// TestPluginRegistry in the catalog.
    PluginCatalog manager;
    ASSERT_TRUE(RegistryCatalog::instance().contains("TestPluginRegistry"));
    ASSERT_FALSE(TestPluginRegistry::instance().contains("TestEntry"));

    manager.load({NES_TEST_PLUGIN_PATH});
    ASSERT_EQ(manager.loadedPlugins().size(), 1);
    const auto entry = TestPluginRegistry::instance().find("TestEntry");
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ((*entry)(), "TestEntry");

    /// Re-loading an already-loaded plugin is an idempotent no-op, not a duplicate registration.
    manager.load({NES_TEST_PLUGIN_PATH});
    ASSERT_EQ(manager.loadedPlugins().size(), 1);
}

TEST(PluginCatalogTest, LoadingNonexistentPluginThrows)
{
    PluginCatalog manager;
    EXPECT_THROW(manager.load({"/nonexistent/libnes-no-such-plugin.so"}), Exception);
}

/// The registration policy lives in the PluginLoader, not in the plugins: an entry for a
/// registry unknown to this host is skipped if optional, and fails the load if required.
TEST(PluginCatalogTest, OptionalEntriesAreSkippedForUnknownRegistries)
{
    PluginDescriptor descriptor{
        .name = "test-descriptor",
        .registries = {},
        .entries
        = {EntryProvision{.registryClassName = "NoSuchRegistry", .key = "SomeKey", .entry = std::any{}, .optional = true}}};
    EXPECT_NO_THROW(PluginLoader::registerPlugins({descriptor}));

    descriptor.entries.front().optional = false;
    EXPECT_THROW(PluginLoader::registerPlugins({descriptor}), Exception);
}

}
