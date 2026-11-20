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
/// in a VARSIZED field, as a file path (`LLM.IMAGE_ENCODING` 'PATH', the default: `SEM_CALL` reads the file when it builds
/// the request) or as the image bytes in base64 ('BASE64': no file is read). Either way the image goes to the model as a
/// base64 data URL in the chat message, the OpenAI vision format that vLLM, Ollama and the hosted APIs accept.

/// `LLM.IMAGE_ENCODING`: what an image field's value is.
enum class ImageEncoding : uint8_t
{
    /// A file path, absolute or relative to `LLM.IMAGE_ROOT`.
    Path,
    /// The image bytes as padded standard base64 (RFC 4648 section 4), no data URL prefix, no line breaks.
    Base64,
};

/// 'PATH' or 'BASE64', case-insensitive; nothing for anything else.
[[nodiscard]] std::optional<ImageEncoding> parseImageEncoding(std::string_view name);
[[nodiscard]] std::string_view imageEncodingName(ImageEncoding encoding);

/// `LLM.IMAGE_MAX_BYTES` when the model sets none.
constexpr size_t DefaultImageMaxBytes = size_t{8} * 1024 * 1024;

/// One image as it goes to the model: its MIME type and its raw bytes, or, for an image that arrived in base64
/// (`ImageEncoding::Base64`), that base64 text as it is and no raw bytes, so it is neither decoded nor encoded again.
struct ImageAttachment
{
    std::string mime;
    std::string bytes;
    std::string base64;

    bool operator==(const ImageAttachment&) const = default;
};

/// The MIME type of an image file from its extension, case-insensitive: jpg/jpeg, png, webp and gif. Nothing for
/// any other extension; the model servers accept only these.
[[nodiscard]] std::optional<std::string> imageMimeType(std::string_view path);

/// Reads the image a row names. A relative `path` is resolved against `root` when that is set, else against the
/// worker's working directory. The error says why the image cannot be used: the file is missing or unreadable, is not
/// a regular file, has an unsupported extension, is empty, or is larger than `maxBytes`.
[[nodiscard]] std::expected<ImageAttachment, std::string> loadImage(std::string_view path, std::string_view root, size_t maxBytes);

/// The image a row carries as base64 (`ImageEncoding::Base64`). The text is checked, not decoded: padded standard base64,
/// at most `maxBytes` once decoded, and its first bytes the signature of a JPEG, PNG, WebP or GIF, which gives the MIME
/// type. The error says why the image cannot be used; it never contains the text itself.
[[nodiscard]] std::expected<ImageAttachment, std::string> imageFromBase64(std::string text, size_t maxBytes);

/// The MIME type from the first bytes of an image file (JPEG, PNG, WebP, GIF); nothing for anything else.
[[nodiscard]] std::optional<std::string> imageMimeTypeOfBytes(std::string_view head);

/// The size `text` decodes to, without decoding it; nothing if it is not padded standard base64.
[[nodiscard]] std::optional<size_t> base64DecodedSize(std::string_view text);

[[nodiscard]] std::string base64Encode(std::string_view bytes);

/// Nothing if `text` is not padded standard base64.
[[nodiscard]] std::optional<std::string> base64Decode(std::string_view text);

/// `data:<mime>;base64,<bytes>`; an image that arrived in base64 keeps its text.
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
