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

#include <cstddef>
#include <span>
#include <utility>
#include <variant>
#include <vector>

#include <Async/AsyncOperatorExecutor.hpp>
#include <Async/AsyncRecordLayout.hpp>
#include <Async/HttpOperatorExecutor.hpp>
#include <Http/HttpBackend.hpp>

namespace NES
{

/// Executes `HTTP_CALL(field, 'url')`: POSTs the field's value as the request body and writes the
/// response body into the output field.
///
/// Everything that makes this work asynchronously -- the thread the call blocks on, the backend per
/// thread, the retries, the ordering of the results -- belongs to the framework and to
/// `HttpOperatorExecutor`. What is left here is about forty lines, which is the point: this is what
/// it costs to put an operator that waits on an HTTP service onto the framework.
class HttpCallExecutor final : public HttpOperatorExecutor<>
{
public:
    explicit HttpCallExecutor(AsyncOperatorContext context);

protected:
    std::pair<HttpRequest, std::monostate> buildRequest(std::span<const AsyncRecordView> batch) override;

    std::vector<AsyncRecordResult>
    decode(const HttpResponse& response, const std::monostate& scratch, std::span<const AsyncRecordView> batch) override;

private:
    /// Everything about the request that does not depend on the record, built once.
    HttpRequest request;
    size_t inputFieldIndex;
    size_t outputFieldIndex;
};

}
