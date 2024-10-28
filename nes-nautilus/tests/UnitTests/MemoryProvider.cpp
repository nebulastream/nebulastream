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

#include <memory>
#include <gtest/gtest.h>
#include <nautilus/Engine.hpp>
#include <MemoryProvider.hpp>

namespace NES
{
class MemoryProviderTest : public ::testing::Test
{
};

TEST_F(MemoryProviderTest, BasicConstruction)
{
    auto buffer = std::make_unique<int8_t[]>(8192);
    *std::bit_cast<int32_t*>(buffer.get()) = 53;

    const std::function compiledFunction = [&](nautilus::val<int8_t*> buffer)
    {
        MemoryProvider provider{buffer};
        auto value = provider[0][0];
        return value.cast<nautilus::val<int32_t>>();
    };

    nautilus::engine::Options options;
    // options.setOption("engine.Compilation", false);
    const nautilus::engine::NautilusEngine engine(options);
    auto executable = engine.registerFunction(compiledFunction);

    EXPECT_EQ(executable(buffer.get()), 53);
}
}