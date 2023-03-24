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


#include <Synopses/AbstractSynopses.hpp>
#include <Synopses/Samples/SampleRandomWithReplacement.hpp>
#include <Execution/Aggregation/AvgAggregation.hpp>
#include <Common/PhysicalTypes/DefaultPhysicalTypeFactory.hpp>
#include <Common/DataTypes/DataTypeFactory.hpp>

int main(int, const char*[]) {
    using namespace NES;


    /**
     * 1. Parse yaml file into SynopsisArguments
     * 2. Create synopsis from SynopsisArguments
     * 3. Create input data (maybe read it from a csv file)
     * 4. Run addToSynopsis() and getApproximate()
     * 5. Calculate throughput and write this into a csv file together with the rest of the params
     */



    size_t sampleSize = 1000;
    auto fieldName = "f1";

    PhysicalTypePtr dataType = DefaultPhysicalTypeFactory().getPhysicalType(DataTypeFactory::createInt8());
    auto averageAggregationFunction = std::make_shared<Runtime::Execution::Aggregation::AvgAggregationFunction>(dataType, dataType);

    auto sampleArguments = ASP::SRSWR(sampleSize, fieldName, averageAggregationFunction);
    auto synopsis = ASP::AbstractSynopses::create(sampleArguments);

}