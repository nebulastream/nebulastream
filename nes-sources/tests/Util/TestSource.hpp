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
#include <Sources/SourceHandle.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

class TestSourceControl
{
public:
    void injectEoS();
    void injectData(std::vector<std::byte> data, size_t numberOfTuples);
    void injectError(std::string error);

    bool wasClosed() const;
    bool wasOpened() const;
    bool wasDestroyed() const;

private:
    friend class TestSource;
    std::atomic_bool closed = false;
    std::atomic_bool opened = false;
    std::atomic_bool destroyed = false;

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
    folly::MPMCQueue<control_data> q;
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
        NES::Memory::TupleBuffer& tupleBuffer, const std::shared_ptr<NES::Memory::AbstractBufferProvider>& bufferManager) override;
    void open() override;
    void close() override;
    [[nodiscard]] SourceType getType() const override;
    [[nodiscard]] std::string toString() const override;
    explicit TestSource(const std::shared_ptr<TestSourceControl>& control);
    ~TestSource() override { control_->destroyed.exchange(true); };

private:
    std::shared_ptr<TestSourceControl> control_;
};

std::pair<SourceHandlePtr, std::shared_ptr<TestSourceControl>>
getTestSource(OriginId originId, std::shared_ptr<Memory::AbstractPoolProvider> bufferPool, SourceReturnType::EmitFunction&& emit);

}
