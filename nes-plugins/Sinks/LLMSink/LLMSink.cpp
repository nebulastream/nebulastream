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

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <span>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>
#include <utility>

#include <SinksParsing/JSONFormat.hpp>
#include <curl/curl.h>
#include <curl/easy.h>
#include <nlohmann/json.hpp>  // or your favorite JSON library
using json = nlohmann::json;
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <Util/Logger/Logger.hpp>
#include <BackpressureChannel.hpp>
#include <ErrorHandling.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES
{

LLMSink::LLMSink(BackpressureController backpressureController, const SinkDescriptor& sinkDescriptor)
    : Sink(std::move(backpressureController)), curl(nullptr)
{
    url = fmt::format(
        "http://{}:{}/{}",
        sinkDescriptor.getFromConfig(ConfigParametersLLM::IPADDRESS),
        sinkDescriptor.getFromConfig(ConfigParametersLLM::PORT),
        sinkDescriptor.getFromConfig(ConfigParametersLLM::ENDPOINT));

    query = sinkDescriptor.getFromConfig(ConfigParametersLLM::QUERY);
    /// We default to json output format here since it seems to be more accesible for further usage of the data
    formatter = std::make_unique<JSONFormat>(*sinkDescriptor.getSchema());
}

std::ostream& LLMSink::toString(std::ostream& str) const
{
    str << fmt::format("LLMSink(url: {})", url);
    str << fmt::format("LLMSink(query: {})", url);

    return str;
}

void LLMSink::start(PipelineExecutionContext&)
{
    NES_DEBUG("Setting up http sink: {}", *this);
    curl = curl_easy_init();
    PRECONDITION(curl, "curl_easy_init failed");
    isOpen = true;

    std::string fBuffer= query+ "\n";

    // Create the payload
    /*json payload;
    payload["query_id"] = "1";
    payload["type"] = "SEM_MAP";
    payload["config"] = {
        {"prompt_template", "Classify sentiment of the following text:{HEARTRATES$DESCRIPTION}. Response only in JSON format. Example:{{\"sentiment\": \"negative\"}}"},
        {"output_column", "sentiment"},
        {"depends_on", json::array({"HEARTRATES$DESCRIPTION"})}
    };
    payload["model"] = "ollama";

    // Convert to string (NDJSON = newline-delimited JSON, so one object per line)
     fBuffer = payload.dump()
    */
    // Your existing curl code
    curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/x-ndjson");

    std::string init_url = url + "/initialize";

    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, init_url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, fBuffer.c_str());

    curl_easy_perform(curl);
    curl_slist_free_all(headers);
}

void LLMSink::execute(const TupleBuffer& inputTupleBuffer, PipelineExecutionContext&)
{
    PRECONDITION(inputTupleBuffer, "Invalid input buffer in LLMSink.");
    PRECONDITION(isOpen, "Sink was not opened");

    /// Format buffer
    const auto fBuffer = formatter->getFormattedBuffer(inputTupleBuffer);

    curl_slist* headers = nullptr;
    /// Set newline delimited json as content type
    headers = curl_slist_append(headers, "Content-Type: application/x-ndjson");

    std::string data_url = url + "/process";
    /// Setup the url, headers and message
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_URL, data_url.c_str());
    curl_easy_setopt(curl, CURLOPT_POST, 1);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, fBuffer.c_str());

    /// Send of message and free headers
    curl_easy_perform(curl);
    curl_slist_free_all(headers);
}

void LLMSink::stop(PipelineExecutionContext&)
{
    NES_DEBUG("Closing http sink.");
    curl_easy_cleanup(curl);
}

DescriptorConfig::Config LLMSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return DescriptorConfig::validateAndFormat<ConfigParametersLLM>(std::move(config), NAME);
}

SinkValidationRegistryReturnType RegisterLLMSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return LLMSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType RegisterLLMSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<LLMSink>(std::move(sinkRegistryArguments.backpressureController), sinkRegistryArguments.sinkDescriptor);
}
}
