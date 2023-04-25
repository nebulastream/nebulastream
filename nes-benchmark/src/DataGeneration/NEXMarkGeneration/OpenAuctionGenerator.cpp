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

#include <API/Schema.hpp>
#include <DataGeneration/NEXMarkGeneration/OpenAuctionGenerator.hpp>

namespace NES::Benchmark::DataGeneration {

SchemaPtr OpenAuctionGenerator::getSchema() {
    return Schema::create()
        ->addField(createField("id", BasicType::UINT64))
        ->addField(createField("seller", BasicType::UINT64))
        ->addField(createField("category", BasicType::UINT16))
        ->addField(createField("quantity", BasicType::UINT8))
        ->addField(createField("type", BasicType::TEXT))
        ->addField(createField("startTime", BasicType::UINT64))
        ->addField(createField("endTime", BasicType::UINT64))
        ->addField(createField("payload", BasicType::UINT64))
        ->addField(createField("payload", BasicType::UINT64))
        ->addField(createField("timestamp", BasicType::UINT64));
}

std::string OpenAuctionGenerator::getName() { return "NEXMarkOpenAuction"; }

std::string OpenAuctionGenerator::toString() {
    std::ostringstream oss;

    oss << getName();

    return oss.str();
}

} // namespace NES::Benchmark::DataGeneration
