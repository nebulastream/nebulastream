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

#include <Report.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <ranges>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>

#include <openssl/evp.h>

#include <ErrorHandling.hpp>

#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>

/// `NES::ErrorCode` is `uint16_t`-backed with sparse values starting at 1000 —
/// outside magic_enum's default `[-128, 128)` scan range. The canonical fix
/// lives in `nes-common/src/ErrorHandling.cpp:236-242` as a TU-private
/// specialization; we replicate it here so `magic_enum::enum_name(ErrorCode)`
/// resolves the right range.
namespace magic_enum::customize
{
template <>
struct enum_range<NES::ErrorCode>
{
    static constexpr int min = 1000;
    static constexpr int max = 10000;
};
}

/// `c` (current char), `i` (digest byte index), `len` and `out` are the canonical
/// scanner / hex-encoder loop names; banding the file keeps them.
namespace NES::ConnTest
{

std::string jsonEscape(const std::string_view input)
{
    std::string out;
    out.reserve(input.size() + 2);
    for (const char controlChar : input)
    {
        switch (controlChar)
        {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (constexpr unsigned char asciiPrintableMin = 0x20; static_cast<unsigned char>(controlChar) < asciiPrintableMin)
                {
                    out += fmt::format("\\u{:04x}", static_cast<unsigned>(controlChar));
                }
                else
                {
                    out += controlChar;
                }
        }
    }
    return out;
}

std::string errorReplyLine(const ErrorInfo& err)
{
    return fmt::format(
        R"({{"ok":false,"phase":"{}","origin":"{}","code":{},"name":"{}","message":"{}"}})",
        jsonEscape(err.phase),
        jsonEscape(err.origin),
        err.code,
        jsonEscape(err.name),
        jsonEscape(err.message));
}

ErrorInfo connectorErrorInfo(const std::string_view phase, const Exception& exception)
{
    return ErrorInfo{
        .origin = "connector",
        .phase = std::string{phase},
        .name = std::string{magic_enum::enum_name(exception.code())},
        .message = exception.what(),
        .code = static_cast<std::uint16_t>(exception.code())};
}

ErrorInfo harnessErrorInfo(const std::string_view phase, const std::string_view name, const std::string_view msg)
{
    return ErrorInfo{.origin = "harness", .phase = std::string{phase}, .name = std::string{name}, .message = std::string{msg}, .code = 0};
}

void captureConnector(Report& report, const std::string_view phase, const Exception& exception)
{
    report.error = connectorErrorInfo(phase, exception);
}

void captureHarness(Report& report, const std::string_view phase, const std::string_view name, const std::string_view msg)
{
    report.error = harnessErrorInfo(phase, name, msg);
}

Sha256Digest::Sha256Digest() : ctx(EVP_MD_CTX_new())
{
    if (ctx == nullptr || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1)
    {
        throw std::runtime_error("EVP_DigestInit_ex(EVP_sha256) failed");
    }
}

Sha256Digest::~Sha256Digest()
{
    if (ctx != nullptr)
    {
        EVP_MD_CTX_free(ctx);
    }
}

void Sha256Digest::update(const void* data, const std::size_t size) const
{
    if (EVP_DigestUpdate(ctx, data, size) != 1)
    {
        throw std::runtime_error("EVP_DigestUpdate failed");
    }
}

std::string Sha256Digest::finalizeHex() const
{
    std::array<unsigned char, EVP_MAX_MD_SIZE> buf{};
    unsigned int len = 0;
    if (EVP_DigestFinal_ex(ctx, buf.data(), &len) != 1)
    {
        throw std::runtime_error("EVP_DigestFinal_ex failed");
    }
    const std::span digest{buf.data(), len};
    return fmt::format(
        "{:02x}", fmt::join(digest | std::views::transform([](unsigned char b) { return std::to_integer<unsigned>(std::byte{b}); }), ""));
}

}
