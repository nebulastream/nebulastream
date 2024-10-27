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
#include <memory>
#include <span>
#include <fcntl.h>
#include <Sources/SourceHandle.hpp>
#include <Sources/SourceProvider.hpp>
#include <Util/Logger/Logger.hpp>
#include <folly/MPMCQueue.h>
#include <gmock/gmock-function-mocker.h>

namespace NES::Sources
{

class TestSourceControl
{
public:
    bool injectEoS();
    bool injectData(std::vector<std::byte> data, size_t numberOfTuples);
    bool injectError(std::string error);

    ::testing::AssertionResult waitUntilOpened();
    ::testing::AssertionResult waitUntilClosed();
    ::testing::AssertionResult waitUntilDestroyed();

    bool wasClosed() const;
    bool wasOpened() const;
    bool wasDestroyed() const;

    void failDuringOpen(std::chrono::milliseconds blockFor = std::chrono::milliseconds(0));
    void failDuringClose(std::chrono::milliseconds blockFor = std::chrono::milliseconds(0));

private:
    friend class TestSource;
    std::promise<void> open;
    std::promise<void> close;
    std::promise<void> destroyed;
    std::atomic_bool failed;

    std::shared_future<void> openFuture = open.get_future().share();
    std::shared_future<void> closeFuture = close.get_future().share();
    std::shared_future<void> destroyedFuture = destroyed.get_future().share();

    bool fail_during_open = false;
    bool fail_during_close = false;
    std::atomic<std::chrono::milliseconds> fail_during_open_duration;
    std::atomic<std::chrono::milliseconds> fail_during_close_duration;

    struct EoS
    {
    };
    struct Data
    {
        std::vector<std::byte> data;
        size_t numberOfTuples;
    };
    struct Error
    {
        std::string error;
    };
    using control_data = std::variant<EoS, Data, Error>;
    folly::MPMCQueue<control_data> q{10};
};


template <typename... T>
struct overloaded : T...
{
    using T::operator()...;
};

class TestSource : public Source
{
public:
    bool fillTupleBuffer(
        NES::Memory::TupleBuffer& tupleBuffer,
        NES::Memory::AbstractBufferProvider& bufferManager,
        std::shared_ptr<Schema> schema,
        const std::stop_token& stopToken) override;
    void open() override;
    void close() override;

protected:
    [[nodiscard]] std::ostream& toString(std::ostream& str) const override;

public:
    explicit TestSource(OriginId sourceId, const std::shared_ptr<TestSourceControl>& control);
    ~TestSource() override;

private:
    OriginId sourceId;
    std::shared_ptr<TestSourceControl> control_;
};

std::pair<std::unique_ptr<SourceHandle>, std::shared_ptr<TestSourceControl>>
getTestSource(OriginId originId, std::shared_ptr<Memory::AbstractPoolProvider> bufferPool);

}
