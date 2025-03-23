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

#include <memory>
#include <SourceCatalogs/SourceCatalog.hpp>

namespace NES::LegacyOptimizer
{
class SemanticQueryValidation
{
public:
    void validate(QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog) const;

private:
    void sinkOperatorValidityCheck(QueryPlan& queryPlan) const;
    void logicalSourceValidityCheck(QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog) const;
    void physicalSourceValidityCheck(QueryPlan& queryPlan, Catalogs::Source::SourceCatalog& sourceCatalog) const;
};
}
