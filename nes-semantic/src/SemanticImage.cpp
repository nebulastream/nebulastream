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

#include <SemanticImage.hpp>

#include <algorithm>
#include <array>
#include <cctype>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <fstream>
#include <ios>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

namespace NES
{

namespace
{

constexpr std::string_view Base64Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
constexpr std::string_view DataUrlPrefix = "data:";
constexpr std::string_view Base64Marker = ";base64,";

/// The index of `character` in the base64 alphabet, or -1.
int base64Value(const char character)
{
    if (character >= 'A' && character <= 'Z')
    {
        return character - 'A';
    }
    if (character >= 'a' && character <= 'z')
    {
        return character - 'a' + 26;
    }
    if (character >= '0' && character <= '9')
    {
        return character - '0' + 52;
    }
    if (character == '+')
    {
        return 62;
    }
    if (character == '/')
    {
        return 63;
    }
    return -1;
}

}

std::optional<std::string> imageMimeType(const std::string_view path)
{
    const auto extension = std::filesystem::path{path}.extension().string();
    std::string lower;
    lower.reserve(extension.size());
    std::ranges::transform(
        extension, std::back_inserter(lower), [](const unsigned char character) { return static_cast<char>(std::tolower(character)); });
    if (lower == ".jpg" || lower == ".jpeg")
    {
        return "image/jpeg";
    }
    if (lower == ".png")
    {
        return "image/png";
    }
    if (lower == ".webp")
    {
        return "image/webp";
    }
    if (lower == ".gif")
    {
        return "image/gif";
    }
    return std::nullopt;
}

std::expected<ImageAttachment, std::string> loadImage(const std::string_view path, const std::string_view root, const size_t maxBytes)
{
    if (path.empty())
    {
        return std::unexpected{std::string{"the image path is empty"}};
    }
    auto mime = imageMimeType(path);
    if (!mime)
    {
        return std::unexpected{fmt::format("'{}' has no image extension (jpg, jpeg, png, webp or gif)", path)};
    }
    std::filesystem::path file{path};
    if (file.is_relative() && !root.empty())
    {
        file = std::filesystem::path{root} / file;
    }
    std::error_code error;
    if (!std::filesystem::is_regular_file(file, error))
    {
        return std::unexpected{fmt::format("'{}' is not a readable file{}", file.string(), error ? ": " + error.message() : "")};
    }
    const auto size = std::filesystem::file_size(file, error);
    if (error)
    {
        return std::unexpected{fmt::format("cannot read the size of '{}': {}", file.string(), error.message())};
    }
    if (size == 0)
    {
        return std::unexpected{fmt::format("'{}' is empty", file.string())};
    }
    if (size > maxBytes)
    {
        return std::unexpected{fmt::format("'{}' has {} bytes, more than LLM.IMAGE_MAX_BYTES {}", file.string(), size, maxBytes)};
    }
    std::ifstream stream{file, std::ios::binary};
    std::string bytes(static_cast<size_t>(size), '\0');
    if (!stream || !stream.read(bytes.data(), static_cast<std::streamsize>(size)))
    {
        return std::unexpected{fmt::format("cannot read '{}'", file.string())};
    }
    return ImageAttachment{.mime = std::move(mime).value(), .bytes = std::move(bytes)};
}

std::string base64Encode(const std::string_view bytes)
{
    std::string encoded;
    encoded.reserve((bytes.size() + 2) / 3 * 4);
    size_t position = 0;
    for (; position + 2 < bytes.size(); position += 3)
    {
        const uint32_t triple = (static_cast<uint32_t>(static_cast<unsigned char>(bytes[position])) << 16U)
            | (static_cast<uint32_t>(static_cast<unsigned char>(bytes[position + 1])) << 8U)
            | static_cast<uint32_t>(static_cast<unsigned char>(bytes[position + 2]));
        encoded.push_back(Base64Alphabet[(triple >> 18U) & 0x3FU]);
        encoded.push_back(Base64Alphabet[(triple >> 12U) & 0x3FU]);
        encoded.push_back(Base64Alphabet[(triple >> 6U) & 0x3FU]);
        encoded.push_back(Base64Alphabet[triple & 0x3FU]);
    }
    const size_t rest = bytes.size() - position;
    if (rest > 0)
    {
        uint32_t triple = static_cast<uint32_t>(static_cast<unsigned char>(bytes[position])) << 16U;
        if (rest == 2)
        {
            triple |= static_cast<uint32_t>(static_cast<unsigned char>(bytes[position + 1])) << 8U;
        }
        encoded.push_back(Base64Alphabet[(triple >> 18U) & 0x3FU]);
        encoded.push_back(Base64Alphabet[(triple >> 12U) & 0x3FU]);
        encoded.push_back(rest == 2 ? Base64Alphabet[(triple >> 6U) & 0x3FU] : '=');
        encoded.push_back('=');
    }
    return encoded;
}

std::optional<std::string> base64Decode(const std::string_view text)
{
    if (text.size() % 4 != 0)
    {
        return std::nullopt;
    }
    std::string decoded;
    decoded.reserve(text.size() / 4 * 3);
    for (size_t position = 0; position < text.size(); position += 4)
    {
        const bool last = position + 4 == text.size();
        std::array<int, 4> values{};
        size_t padding = 0;
        for (size_t offset = 0; offset < 4; ++offset)
        {
            const char character = text[position + offset];
            if (character == '=' && last && offset >= 2)
            {
                ++padding;
                values.at(offset) = 0;
                continue;
            }
            /// Padding only at the very end, and nothing but padding after it.
            if (padding > 0)
            {
                return std::nullopt;
            }
            values.at(offset) = base64Value(character);
            if (values.at(offset) < 0)
            {
                return std::nullopt;
            }
        }
        const uint32_t triple = (static_cast<uint32_t>(values[0]) << 18U) | (static_cast<uint32_t>(values[1]) << 12U)
            | (static_cast<uint32_t>(values[2]) << 6U) | static_cast<uint32_t>(values[3]);
        decoded.push_back(static_cast<char>((triple >> 16U) & 0xFFU));
        if (padding < 2)
        {
            decoded.push_back(static_cast<char>((triple >> 8U) & 0xFFU));
        }
        if (padding < 1)
        {
            decoded.push_back(static_cast<char>(triple & 0xFFU));
        }
    }
    return decoded;
}

std::string imageDataUrl(const ImageAttachment& image)
{
    return fmt::format("{}{}{}{}", DataUrlPrefix, image.mime, Base64Marker, base64Encode(image.bytes));
}

std::optional<ImageAttachment> parseImageDataUrl(const std::string_view url)
{
    if (!url.starts_with(DataUrlPrefix))
    {
        return std::nullopt;
    }
    const auto marker = url.find(Base64Marker);
    if (marker == std::string_view::npos)
    {
        return std::nullopt;
    }
    auto bytes = base64Decode(url.substr(marker + Base64Marker.size()));
    if (!bytes)
    {
        return std::nullopt;
    }
    return ImageAttachment{
        .mime = std::string{url.substr(DataUrlPrefix.size(), marker - DataUrlPrefix.size())}, .bytes = std::move(bytes).value()};
}

std::string imagePlaceholder(const size_t index)
{
    return fmt::format("[image {}]", index + 1);
}

nlohmann::json buildChatRequest(
    const std::string_view modelName, const std::string_view prompt, const std::span<const ImageAttachment> images, const size_t maxTokens)
{
    nlohmann::json content = std::string{prompt};
    if (!images.empty())
    {
        content = nlohmann::json::array({{{"type", "text"}, {"text", std::string{prompt}}}});
        for (const auto& image : images)
        {
            content.push_back({{"type", "image_url"}, {"image_url", {{"url", imageDataUrl(image)}}}});
        }
    }
    nlohmann::json request{
        {"model", std::string{modelName}},
        {"messages", nlohmann::json::array({{{"role", "user"}, {"content", std::move(content)}}})},
        {"stream", false}};
    if (maxTokens > 0)
    {
        request["max_tokens"] = maxTokens;
    }
    return request;
}

std::vector<ImageAttachment> imagesOfChatRequest(const nlohmann::json& request)
{
    std::vector<ImageAttachment> images;
    if (!request.contains("messages") || !request["messages"].is_array())
    {
        return images;
    }
    for (const auto& message : request["messages"])
    {
        if (!message.is_object() || !message.contains("content") || !message["content"].is_array())
        {
            continue;
        }
        for (const auto& part : message["content"])
        {
            if (part.is_object() && part.value("type", "") == "image_url" && part.contains("image_url") && part["image_url"].is_object()
                && part["image_url"].contains("url") && part["image_url"]["url"].is_string())
            {
                if (auto image = parseImageDataUrl(part["image_url"]["url"].get<std::string>()))
                {
                    images.push_back(std::move(image).value());
                }
            }
        }
    }
    return images;
}

uint64_t fnv1a64(const std::string_view bytes)
{
    constexpr uint64_t offsetBasis = 14695981039346656037ULL;
    constexpr uint64_t prime = 1099511628211ULL;
    uint64_t hash = offsetBasis;
    for (const char character : bytes)
    {
        hash ^= static_cast<unsigned char>(character);
        hash *= prime;
    }
    return hash;
}

}
