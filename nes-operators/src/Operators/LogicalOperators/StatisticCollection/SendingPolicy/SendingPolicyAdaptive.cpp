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

#include <StatisticIdentifiers.hpp>
#include <Operators/LogicalOperators/StatisticCollection/SendingPolicy/SendingPolicyAdaptive.hpp>

namespace NES::Statistic {

std::string SendingPolicyAdaptive::toString() const { return "ADAPTIVE"; }

SendingPolicyPtr SendingPolicyAdaptive::create(StatisticDataCodec sinkDataCodec) {
    return std::make_shared<SendingPolicyAdaptive>(SendingPolicyAdaptive(sinkDataCodec));
}

bool SendingPolicyAdaptive::operator==(const SendingPolicy& rhs) const {
    return rhs.instanceOf<const SendingPolicyAdaptive>();
}

SendingPolicyAdaptive::SendingPolicyAdaptive(StatisticDataCodec sinkDataCodec) : SendingPolicy(sinkDataCodec) {}

}// namespace NES::Statistic