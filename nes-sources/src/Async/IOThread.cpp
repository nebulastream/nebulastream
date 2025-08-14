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

#include <Async/IOThread.hpp>

#include <boost/asio/executor_work_guard.hpp>

#include <Util/Logger/Logger.hpp>

namespace NES::Sources
{

/// The io_context is initialized first from its default constructor.
IOThread::IOThread() : workGuard(asio::make_work_guard(ioc)), ioThread([this] { ioc.run(); })
{
    NES_DEBUG("IOThread: started [{}]", ioThread.get_id());
}

IOThread::~IOThread()
{
    workGuard.reset();
    ioc.stop();
    NES_DEBUG("IOThread: stopped [{}]", ioThread.get_id());
    /// Thread is joined when leaving this scope
}

}
