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

#include "Operators/LogicalOperators/Statistics/CountMinDescriptor.hpp"
#include "API/Schema.hpp"

namespace NES::Experimental::Statistics {

CountMinDescriptor::CountMinDescriptor(double error, double probability) : WindowStatisticDescriptor(), error(error), probability(probability) {}

bool CountMinDescriptor::operator==(WindowStatisticDescriptor& statisticsDescriptor) {
    auto countMinDesc = dynamic_cast<CountMinDescriptor*>(&statisticsDescriptor);
    if (countMinDesc != nullptr) {
        if (error == countMinDesc->getError() && probability == countMinDesc->getProbability()) {
            return true;
        }
        return false;
    }
    return false;
}

void CountMinDescriptor::addStatisticFields(NES::SchemaPtr schema) {
    schema->addField("error", BasicType::FLOAT64);
    schema->addField("probability", BasicType::FLOAT64);
}

double CountMinDescriptor::getError() const { return error; }

double CountMinDescriptor::getProbability() const { return probability; }

}
