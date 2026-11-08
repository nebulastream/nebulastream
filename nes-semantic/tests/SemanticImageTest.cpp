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

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ios>
#include <string>
#include <vector>

#include <Identifiers/Identifier.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <BaseUnitTest.hpp>
#include <MockSemanticBackend.hpp>
#include <SemanticCallPayload.hpp>
#include <SemanticModelCatalog.hpp>

namespace NES
{

namespace
{

/// Every byte value once, so encoding and decoding see each of them.
std::string allBytes()
{
    std::string bytes;
    for (int value = 0; value < 256; ++value)
    {
        bytes.push_back(static_cast<char>(value));
    }
    return bytes;
}

void writeFile(const std::filesystem::path& path, const std::string& bytes)
{
    std::ofstream stream{path, std::ios::binary};
    stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}

class SemanticImageTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite() { Logger::setupLogging("SemanticImageTest.log", LogLevel::LOG_DEBUG); }

    void SetUp() override
    {
        Testing::BaseUnitTest::SetUp();
        directory = std::filesystem::temp_directory_path()
            / fmt::format("nes-semantic-image-test-{}", ::testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(directory);
        std::filesystem::create_directories(directory / "images");
    }

    void TearDown() override
    {
        std::filesystem::remove_all(directory);
        Testing::BaseUnitTest::TearDown();
    }

protected:
    std::filesystem::path directory;
};

TEST_F(SemanticImageTest, MimeTypeFollowsTheExtension)
{
    EXPECT_EQ(imageMimeType("a/b/scan.jpg"), "image/jpeg");
    EXPECT_EQ(imageMimeType("scan.JPEG"), "image/jpeg");
    EXPECT_EQ(imageMimeType("scan.png"), "image/png");
    EXPECT_EQ(imageMimeType("scan.WebP"), "image/webp");
    EXPECT_EQ(imageMimeType("scan.gif"), "image/gif");
    EXPECT_EQ(imageMimeType("scan.bmp"), std::nullopt);
    EXPECT_EQ(imageMimeType("scan"), std::nullopt);
    EXPECT_EQ(imageMimeType("jpg"), std::nullopt);
}

TEST_F(SemanticImageTest, Base64MatchesTheStandardAndRoundTrips)
{
    /// RFC 4648, section 10.
    EXPECT_EQ(base64Encode(""), "");
    EXPECT_EQ(base64Encode("f"), "Zg==");
    EXPECT_EQ(base64Encode("fo"), "Zm8=");
    EXPECT_EQ(base64Encode("foo"), "Zm9v");
    EXPECT_EQ(base64Encode("foobar"), "Zm9vYmFy");
    EXPECT_EQ(base64Decode("Zm9vYg=="), "foob");
    EXPECT_EQ(base64Decode("Zm9vYmE="), "fooba");
    const auto bytes = allBytes();
    for (size_t length = 0; length <= bytes.size(); length += 37)
    {
        const auto prefix = bytes.substr(0, length);
        EXPECT_EQ(base64Decode(base64Encode(prefix)), prefix) << length;
    }
    EXPECT_EQ(base64Decode(base64Encode(bytes)), bytes);
}

TEST_F(SemanticImageTest, Base64RejectsMalformedText)
{
    EXPECT_EQ(base64Decode("Zm9"), std::nullopt);
    EXPECT_EQ(base64Decode("Zm9v!A=="), std::nullopt);
    EXPECT_EQ(base64Decode("Zg==Zm9v"), std::nullopt);
    EXPECT_EQ(base64Decode("Z=9v"), std::nullopt);
}

TEST_F(SemanticImageTest, DataUrlRoundTrips)
{
    const ImageAttachment image{.mime = "image/png", .bytes = allBytes()};
    const auto url = imageDataUrl(image);
    EXPECT_TRUE(url.starts_with("data:image/png;base64,"));
    EXPECT_EQ(parseImageDataUrl(url), image);
    EXPECT_EQ(parseImageDataUrl("https://example.org/a.png"), std::nullopt);
    EXPECT_EQ(parseImageDataUrl("data:image/png,raw"), std::nullopt);
}

TEST_F(SemanticImageTest, LoadsRelativeToTheRootOrAbsolute)
{
    const auto bytes = allBytes();
    writeFile(directory / "images" / "lesion.jpg", bytes);
    const auto relative = loadImage("images/lesion.jpg", directory.string(), DefaultImageMaxBytes);
    ASSERT_TRUE(relative.has_value()) << relative.error();
    EXPECT_EQ(relative->mime, "image/jpeg");
    EXPECT_EQ(relative->bytes, bytes);
    /// An absolute path ignores the root.
    const auto absolute = loadImage((directory / "images" / "lesion.jpg").string(), "/nonexistent", DefaultImageMaxBytes);
    ASSERT_TRUE(absolute.has_value()) << absolute.error();
    EXPECT_EQ(absolute->bytes, bytes);
}

TEST_F(SemanticImageTest, ReportsWhyAnImageCannotBeUsed)
{
    writeFile(directory / "images" / "big.png", std::string(100, 'x'));
    writeFile(directory / "images" / "empty.png", "");
    writeFile(directory / "images" / "notes.txt", "text");
    const auto root = directory.string();

    const auto missing = loadImage("images/missing.jpg", root, DefaultImageMaxBytes);
    ASSERT_FALSE(missing.has_value());
    EXPECT_NE(missing.error().find("not a readable file"), std::string::npos) << missing.error();

    const auto tooLarge = loadImage("images/big.png", root, 99);
    ASSERT_FALSE(tooLarge.has_value());
    EXPECT_NE(tooLarge.error().find("IMAGE_MAX_BYTES"), std::string::npos) << tooLarge.error();
    EXPECT_TRUE(loadImage("images/big.png", root, 100).has_value());

    const auto empty = loadImage("images/empty.png", root, DefaultImageMaxBytes);
    ASSERT_FALSE(empty.has_value());
    EXPECT_NE(empty.error().find("empty"), std::string::npos) << empty.error();

    const auto text = loadImage("images/notes.txt", root, DefaultImageMaxBytes);
    ASSERT_FALSE(text.has_value());
    EXPECT_NE(text.error().find("no image extension"), std::string::npos) << text.error();

    std::filesystem::create_directories(directory / "images" / "folder.jpg");
    EXPECT_FALSE(loadImage("images/folder.jpg", root, DefaultImageMaxBytes).has_value());
    EXPECT_FALSE(loadImage("", root, DefaultImageMaxBytes).has_value());
}

TEST_F(SemanticImageTest, TextRequestIsUnchanged)
{
    const auto request = buildChatRequest("gemma4:e2b", "Classify", {}, 0);
    EXPECT_EQ(
        request,
        nlohmann::json::parse(R"({"model": "gemma4:e2b", "messages": [{"role": "user", "content": "Classify"}], "stream": false})"));
    EXPECT_EQ(buildChatRequest("m", "p", {}, 64)["max_tokens"], 64);
}

TEST_F(SemanticImageTest, ImageRequestHasTextThenImageParts)
{
    const std::vector images{
        ImageAttachment{.mime = "image/jpeg", .bytes = "\xff\xd8\xff"}, ImageAttachment{.mime = "image/png", .bytes = allBytes()}};
    const auto request = buildChatRequest("gemma4:e2b", "Is the lesion malignant?", images, 0);
    const auto& content = request["messages"][0]["content"];
    ASSERT_TRUE(content.is_array());
    ASSERT_EQ(content.size(), 3);
    EXPECT_EQ(content[0], nlohmann::json::parse(R"({"type": "text", "text": "Is the lesion malignant?"})"));
    EXPECT_EQ(content[1]["type"], "image_url");
    EXPECT_EQ(content[1]["image_url"]["url"], "data:image/jpeg;base64,/9j/");
    EXPECT_EQ(content[2]["image_url"]["url"], imageDataUrl(images[1]));
    EXPECT_EQ(request["messages"][0]["role"], "user");
    EXPECT_FALSE(request.contains("max_tokens"));
    /// What the mock reads back is exactly what went in.
    EXPECT_EQ(imagesOfChatRequest(request), images);
    EXPECT_TRUE(imagesOfChatRequest(buildChatRequest("m", "p", {}, 0)).empty());
}

TEST_F(SemanticImageTest, PlaceholdersCountFromOne)
{
    EXPECT_EQ(imagePlaceholder(0), "[image 1]");
    EXPECT_EQ(imagePlaceholder(2), "[image 3]");
}

TEST_F(SemanticImageTest, MockDescribesTheImagesItReceived)
{
    const MockSemanticBackend mock{"imageinfo", {"LABEL"}};
    const std::string prompt = "Classify\nData: {\"row1\": \"[image 1]\"}";
    const std::vector images{ImageAttachment{.mime = "image/jpeg", .bytes = "abc"}};
    const auto answer = mock.respond(prompt, images);
    ASSERT_TRUE(answer.has_value());
    const auto parsed = nlohmann::json::parse(answer.value());
    EXPECT_EQ(parsed["row1"]["label"]["answer"], fmt::format("image/jpeg:3:{:016x}", fnv1a64("abc")));
    const auto none = nlohmann::json::parse(mock.respond(prompt).value());
    EXPECT_EQ(none["row1"]["label"]["answer"], "none");
    EXPECT_TRUE(MockSemanticBackend::isValidBehaviour("imageinfo@5"));
}

TEST_F(SemanticImageTest, FnvMatchesTheReferenceValues)
{
    EXPECT_EQ(fnv1a64(""), 0xcbf29ce484222325ULL);
    EXPECT_EQ(fnv1a64("a"), 0xaf63dc4c8601ec8cULL);
}

TEST_F(SemanticImageTest, PayloadCarriesImageFieldsOnlyWhenSet)
{
    SemanticCallPayload payload;
    payload.model = "lesion";
    payload.inputFields = {"IMAGE", "NOTE"};
    payload.outputFields = {"LABEL"};
    payload.config.endpoint = "imageinfo";
    payload.config.modelName = "m";
    payload.config.backend = "mock";
    payload.config.execution = SemanticExecution::ASYNCHRONOUS;
    payload.config.steps = {SemanticStep{
        .kind = SemanticStep::Kind::MAP, .prompt = "Classify", .outputColumn = "LABEL", .outputValues = {}, .defaultValue = "D"}};

    const auto text = encodeSemanticCallPayload(payload);
    EXPECT_EQ(text.find("\"imageFields\""), std::string::npos)
        << "a model without images keeps its payload, and its EXPLAIN hash: " << text;

    payload.config.imageFields = {"IMAGE"};
    payload.config.imageRoot = "/data/skin";
    payload.config.imageMaxBytes = 1234;
    const auto decoded = decodeSemanticCallPayload(encodeSemanticCallPayload(payload));
    EXPECT_EQ(decoded.config.imageFields, std::vector<std::string>{"IMAGE"});
    EXPECT_EQ(decoded.config.imageRoot, "/data/skin");
    EXPECT_EQ(decoded.config.imageMaxBytes, 1234);
    EXPECT_EQ(decoded.config, payload.config);
}

}
