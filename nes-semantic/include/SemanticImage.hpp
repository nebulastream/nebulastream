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
#include <cstdint>
#include <expected>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <nlohmann/json.hpp>

namespace NES
{

/// Image inputs of SEM_MAP and SEM_FILTER (`LLM.IMAGE_FIELDS`, docs/technical/semantic_images.md). A row carries the image
/// as a file path in a VARSIZED field; `SEM_CALL` reads the file when it builds the request and attaches it to the chat
/// message as a base64 data URL, the OpenAI vision format that vLLM, Ollama and the hosted APIs accept.

/// `LLM.IMAGE_MAX_BYTES` when the model sets none.
constexpr size_t DefaultImageMaxBytes = size_t{8} * 1024 * 1024;

/// One image as it goes to the model: its MIME type and its raw bytes.
struct ImageAttachment
{
    std::string mime;
    std::string bytes;

    bool operator==(const ImageAttachment&) const = default;
};

/// The MIME type of an image file from its extension, case-insensitive: jpg/jpeg, png, webp and gif. Nothing for
/// any other extension; the model servers accept only these.
[[nodiscard]] std::optional<std::string> imageMimeType(std::string_view path);

/// Reads the image a row names. A relative `path` is resolved against `root` when that is set, else against the
/// worker's working directory. The error says why the image cannot be used: the file is missing or unreadable, is not
/// a regular file, has an unsupported extension, is empty, or is larger than `maxBytes`.
[[nodiscard]] std::expected<ImageAttachment, std::string> loadImage(std::string_view path, std::string_view root, size_t maxBytes);

[[nodiscard]] std::string base64Encode(std::string_view bytes);

/// Nothing if `text` is not padded standard base64.
[[nodiscard]] std::optional<std::string> base64Decode(std::string_view text);

/// `data:<mime>;base64,<bytes>`.
[[nodiscard]] std::string imageDataUrl(const ImageAttachment& image);

/// The inverse of `imageDataUrl`; nothing for anything else.
[[nodiscard]] std::optional<ImageAttachment> parseImageDataUrl(std::string_view url);

/// What the prompt text shows in place of the n-th image field of a row (0-based `index`): "[image 1]", "[image 2]",
/// in the order the images are attached. The path itself is not shown: it means nothing to the model and may carry the
/// label, as the file names of the skin dataset do.
[[nodiscard]] std::string imagePlaceholder(size_t index);

/// The chat-completion request of one prompt. Without images the user message's content is the prompt text, as for
/// every text model; with images it is a list of parts, the text first, then one `image_url` part per image.
[[nodiscard]] nlohmann::json
buildChatRequest(std::string_view modelName, std::string_view prompt, std::span<const ImageAttachment> images, size_t maxTokens);

/// The images a chat request built by `buildChatRequest` carries, in order. Lets the mock answer from exactly the
/// payload the HTTP backend would send.
[[nodiscard]] std::vector<ImageAttachment> imagesOfChatRequest(const nlohmann::json& request);

/// 64-bit FNV-1a, a checksum the mock reports for each image it received so tests can compare it with the file's.
[[nodiscard]] uint64_t fnv1a64(std::string_view bytes);

}
