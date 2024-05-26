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

#include <FormattedDataGenerator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <any>
#include <generation/JSONAdapter.hpp>
#include <generation/SchemaBuffer.hpp>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-more-matchers.h>
#include <gtest/gtest.h>
#include <scn/detail/scan.h>

using namespace std::literals;

class JSONFormatTest : public ::testing::Test {
  protected:
    void SetUp() override { NES::Logger::setupLogging("JSONFormatTest.log", NES::LogLevel::LOG_DEBUG); }

  protected:
    constexpr static size_t BUFFER_SIZE = 8192;
};

TEST_F(JSONFormatTest, BasicSchemaTest) {
    using Schema = NES::DataParser::StaticSchema<NES::DataParser::Field<NES::DataParser::UINT8, "a">,
                                                 NES::DataParser::Field<NES::DataParser::FLOAT32, "b">,
                                                 NES::DataParser::Field<NES::DataParser::TEXT, "c">,
                                                 NES::DataParser::Field<NES::DataParser::UINT32, "d">,
                                                 NES::DataParser::Field<NES::DataParser::INT64, "e">>;
    using SchemaBuffer = NES::DataParser::SchemaBuffer<Schema, 8192>;
    using Adapter = NES::DataParser::JSON::JSONAdapter<SchemaBuffer>;

    auto data = generateJSONDataRandomOrder<uint8_t, float, std::string, uint32_t, int64_t>(4);

    auto bm = std::make_shared<NES::Runtime::BufferManager>();
    auto buffer = bm->getBufferBlocking();

    auto rest = Adapter::parseIntoBuffer(data, buffer);

    EXPECT_EQ(buffer.getNumberOfTuples(), 4);
    EXPECT_THAT(rest, ::testing::IsEmpty());
}
