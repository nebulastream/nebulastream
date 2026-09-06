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

#include <LLMSink.hpp>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <curl/curl.h>
#include <curl/easy.h>
#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/BufferIterator.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES
{

namespace
{
/// Swallow the operator's response body instead of letting curl print it to stdout.
size_t discardResponse(char*, const size_t size, const size_t nmemb, void*)
{
    return size * nmemb;
}
}

LLMSink::LLMSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController)), curl(nullptr)
{
    url = fmt::format(
        "http://{}:{}/{}",
        sinkDescriptor.getFromConfig(ConfigParametersLLM::IP_ADDRESS),
        sinkDescriptor.getFromConfig(ConfigParametersLLM::PORT),
        sinkDescriptor.getFromConfig(ConfigParametersLLM::ENDPOINT));

    query = sinkDescriptor.getFromConfig(ConfigParametersLLM::QUERY);
}

std::ostream& LLMSink::toString(std::ostream& str) const
{
    str << fmt::format("LLMSink(url: {}, query: {})", url, query);
    return str;
}

void LLMSink::post(const std::string_view path, const std::string& body) const
{
    const std::string target = fmt::format("{}/{}", url, path);

    curl_slist* headers = curl_slist_append(nullptr, "Content-Type: application/x-ndjson");
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, target.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1L);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
    curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, static_cast<long>(body.size()));
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, discardResponse);

    const CURLcode result = curl_easy_perform(curl);
    long status = 0;
    if (result == CURLE_OK)
    {
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    }
    curl_slist_free_all(headers);

    /// The original version discarded this return code entirely, so an
    /// unreachable operator was indistinguishable from a healthy query.
    if (result != CURLE_OK)
    {
        throw CannotOpenSink("LLMSink: POST {} failed: {}", target, curl_easy_strerror(result));
    }
    if (status < 200 || status >= 300)
    {
        throw CannotOpenSink("LLMSink: POST {} returned HTTP {}", target, status);
    }
}

void LLMSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up LLM sink: {}", *this);
    curl = curl_easy_init();
    PRECONDITION(curl, "curl_easy_init failed");
    isOpen = true;

    /// The QUERY blob is forwarded verbatim; the operator parses it as one
    /// NDJSON line, hence the trailing newline.
    post("initialize", query + "\n");
}

void LLMSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in LLMSink.");
    PRECONDITION(isOpen, "Sink was not opened");

    /// Formatting already happened in the emit phase, so the buffer (and its
    /// children) hold ready-to-send bytes. With SINK.OUTPUT_FORMAT = 'JSON'
    /// that is NDJSON, exactly what /process expects.
    BufferIterator iterator{inputTupleBuffer};
    for (auto element = iterator.getNextElement(); element.has_value(); element = iterator.getNextElement())
    {
        const std::string body(element->buffer.getAvailableMemoryArea<char>().data(), element->contentLength);
        post("process", body);
    }
}

void LLMSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing LLM sink.");
    /// Deliberately does NOT POST /stop. That endpoint calls
    /// Coordinator.unregister_query, which cancels rows still in flight inside
    /// the operator -- and NES stops the sink as soon as the source is
    /// exhausted, i.e. typically while the last batch is still being processed.
    /// Tearing the query down is left to the operator's own lifecycle.
    curl_easy_cleanup(curl);
    curl = nullptr;
    isOpen = false;
}

DescriptorConfig::Config LLMSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersLLM>(std::move(config), NAME);
}
}
