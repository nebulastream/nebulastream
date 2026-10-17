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

#include <Sources/AsyncFunction.hpp>

#include <string>
#include <utility>
#include <vector>
#include <boost/asio/awaitable.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

boost::asio::awaitable<std::string> AsyncFunction::call(std::vector<std::string>)
{
    throw NotImplemented("async function implements neither call nor callBatch");
    co_return std::string{};
}

boost::asio::awaitable<std::vector<AsyncCallResult>> AsyncFunction::callBatch(std::vector<std::vector<std::string>> rows)
{
    std::vector<AsyncCallResult> results;
    results.reserve(rows.size());
    for (auto& arguments : rows)
    {
        results.push_back({co_await call(std::move(arguments))});
    }
    co_return results;
}

}
