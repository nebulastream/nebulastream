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

#include <cstddef>
#include <memory>
#include <span>
#include <type_traits>
#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <SliceStore/NullSpillBackend.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/SpillBackend.hpp>

namespace NES::Testing
{

class NullSpillBackendTest : public BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("NullSpillBackendTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup NullSpillBackendTest test class.");
    }

    void SetUp() override { BaseUnitTest::SetUp(); }
};

/// Test A — put discards without throwing, for both empty and non-empty payloads.
TEST_F(NullSpillBackendTest, putIsNoOp)
{
    NullSpillBackend backend;
    const std::vector<std::byte> payload{std::byte{1}, std::byte{2}, std::byte{3}};

    EXPECT_NO_THROW(backend.put(SliceEnd(Timestamp(123)), WorkerThreadId(0), std::span<const std::byte>{}));
    EXPECT_NO_THROW(backend.put(SliceEnd(Timestamp(456)), WorkerThreadId(7), std::span<const std::byte>{payload}));
}

/// Test B — get always reports a miss, even after a put on the same key.
TEST_F(NullSpillBackendTest, getAlwaysMisses)
{
    NullSpillBackend backend;
    const std::vector<std::byte> payload{std::byte{42}};
    backend.put(SliceEnd(Timestamp(123)), WorkerThreadId(0), std::span<const std::byte>{payload});

    EXPECT_FALSE(backend.get(SliceEnd(Timestamp(123)), WorkerThreadId(0)).has_value());
    EXPECT_FALSE(backend.get(SliceEnd(Timestamp(999)), WorkerThreadId(3)).has_value());
}

/// Test C — erase is a no-op and does not throw.
TEST_F(NullSpillBackendTest, eraseIsNoOp)
{
    NullSpillBackend backend;
    EXPECT_NO_THROW(backend.erase(SliceEnd(Timestamp(123))));
}

/// Phase 2 (2b) — put/get with an explicit partition arg stays a no-op: writes are discarded
/// per partition and every partitioned get reports a miss.
TEST_F(NullSpillBackendTest, partitionedPutGetStaysNoOp)
{
    NullSpillBackend backend;
    const std::vector<std::byte> payload{std::byte{42}};

    EXPECT_NO_THROW(backend.put(SliceEnd(Timestamp(123)), WorkerThreadId(0), std::span<const std::byte>{payload}, 1));
    EXPECT_NO_THROW(backend.put(SliceEnd(Timestamp(123)), WorkerThreadId(0), std::span<const std::byte>{payload}, 2));

    EXPECT_FALSE(backend.get(SliceEnd(Timestamp(123)), WorkerThreadId(0), 1).has_value());
    EXPECT_FALSE(backend.get(SliceEnd(Timestamp(123)), WorkerThreadId(0), 2).has_value());
    EXPECT_FALSE(backend.get(SliceEnd(Timestamp(123)), WorkerThreadId(0), 0).has_value()); /// default partition
}

/// Test D — interface contract: NullSpillBackend is a SpillBackend and is usable
/// polymorphically through the interface pointer (the way later phases consume it).
TEST_F(NullSpillBackendTest, satisfiesInterfaceContract)
{
    static_assert(std::is_base_of_v<SpillBackend, NullSpillBackend>, "NullSpillBackend must implement SpillBackend");

    const std::unique_ptr<SpillBackend> backend = std::make_unique<NullSpillBackend>();
    const std::vector<std::byte> payload{std::byte{1}};

    EXPECT_NO_THROW(backend->put(SliceEnd(Timestamp(1)), WorkerThreadId(0), std::span<const std::byte>{payload}));
    EXPECT_FALSE(backend->get(SliceEnd(Timestamp(1)), WorkerThreadId(0)).has_value());
    EXPECT_NO_THROW(backend->erase(SliceEnd(Timestamp(1))));
}

}
