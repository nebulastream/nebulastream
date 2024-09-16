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

#include "TestSource.hpp"

void NES::Sources::TestSourceControl::injectEoS()
{
    q.blockingWrite(EoS{});
}
void NES::Sources::TestSourceControl::injectData(std::vector<std::byte> data, size_t numberOfTuples)
{
    q.blockingWrite(Data{data, numberOfTuples});
}
void NES::Sources::TestSourceControl::injectError(std::string error)
{
    q.blockingWrite(Error{error});
}
bool NES::Sources::TestSourceControl::wasClosed() const
{
    return closed.load();
}
bool NES::Sources::TestSourceControl::wasOpened() const
{
    return opened.load();
}
bool NES::Sources::TestSourceControl::wasDestroyed() const
{
    return destroyed.load();
}
bool NES::Sources::TestSource::fillTupleBuffer(
    NES::Memory::TupleBuffer& tupleBuffer, const std::shared_ptr<NES::Memory::AbstractBufferProvider>&)
{
    TestSourceControl::control_data cd;
    control_->q.blockingRead(cd);
    auto data = std::visit(
        overloaded{
            [](TestSourceControl::Error e) -> std::optional<TestSourceControl::Data>
            {
                throw std::runtime_error(e.error);
                return std::nullopt;
            },
            [](TestSourceControl::Data d) { return std::optional<TestSourceControl::Data>(d); },
            [](TestSourceControl::EoS) -> std::optional<TestSourceControl::Data> { return std::nullopt; }},
        cd);

    if (!data)
    {
        return false;
    }
    NES_ASSERT(data->data.size() <= tupleBuffer.getBufferSize(), "Test source attempted to send a buffer which is to big");
    tupleBuffer.setNumberOfTuples(data->numberOfTuples);
    std::copy(data->data.begin(), data->data.end(), tupleBuffer.getBuffer<std::byte>());
    return true;
}
void NES::Sources::TestSource::open()
{
    control_->opened.exchange(true);
}
void NES::Sources::TestSource::close()
{
    control_->closed.exchange(true);
}
NES::SourceType NES::Sources::TestSource::getType() const
{
    return SourceType::CSV_SOURCE;
}
std::string NES::Sources::TestSource::toString() const
{
    return "test source";
}
NES::Sources::TestSource::TestSource(const std::shared_ptr<TestSourceControl>& control) : control_(control)
{
}
std::pair<NES::Sources::SourceHandlePtr, std::shared_ptr<NES::Sources::TestSourceControl>> NES::Sources::getTestSource(
    OriginId originId, std::shared_ptr<NES::Memory::AbstractPoolProvider> bufferPool, SourceReturnType::EmitFunction&& emit)
{
    auto ctrl = std::make_shared<TestSourceControl>();
    auto testSource = std::make_unique<TestSource>(ctrl);
    auto sourceHandle = std::make_shared<SourceHandle>(
        std::move(originId), Schema::create(), std::move(bufferPool), std::move(emit), 100, std::move(testSource));
    return {sourceHandle, ctrl};
}
