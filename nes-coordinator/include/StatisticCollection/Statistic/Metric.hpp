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

#ifndef NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICTYPE_STATISTICTYPE_HPP_
#define NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICTYPE_STATISTICTYPE_HPP_
#include <API/Query.hpp>
#include <StatisticIdentifiers.hpp>
namespace NES::Statistic {
/**
 * @brief This class acts as an abstract class for all possible statistic types
 */
class Metric {};

/**
 * @brief Collects statistics to estimate the selectivity
 */
class Selectivity : public Metric {
  public:
    explicit Selectivity(const ExpressionNodePtr& expressionNode) : expressionNode(expressionNode) {}
    const ExpressionNodePtr expressionNode;
};

class Cardinality : public Metric {
  public:
    explicit Cardinality(const std::string& fieldName) : fieldName(fieldName) {}
    const std::string fieldName;
};

class IngestionRate : public Metric {
  public:
    explicit IngestionRate() : fieldName(INGESTION_RATE_FIELD_NAME) {}
    const std::string fieldName;
};

class BufferRate : public Metric {
  public:
    explicit BufferRate() : fieldName(BUFFER_RATE_FIELD_NAME) {}
    const std::string fieldName;
};

class MinVal : public Metric {
  public:
    explicit MinVal(const std::string& fieldName) : fieldName(fieldName) {}
    const std::string fieldName;
};
}// namespace NES::Statistic
#endif//NES_NES_COORDINATOR_INCLUDE_STATISTIC_STATISTICTYPE_STATISTICTYPE_HPP_
