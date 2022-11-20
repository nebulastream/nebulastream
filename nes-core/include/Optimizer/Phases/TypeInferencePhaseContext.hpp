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

#ifndef NES_CORE_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASECONTEXT_HPP_
#define NES_CORE_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASECONTEXT_HPP_

#include <Catalogs/UDF/UdfCatalog.hpp>
#include <memory>

namespace NES::Catalogs {

namespace Source {
class SourceCatalog;
using SourceCatalogPtr = std::shared_ptr<SourceCatalog>;
}// namespace Source

namespace UDF {
class UdfCatalog;
using UdfCatalogPtr = std::shared_ptr<UdfCatalog>;
}// namespace UDF

}// namespace NES::Catalogs

namespace NES::Optimizer {

/**
 * @brief this class is passed to the inferStamp functions of ExpressionNodes. It is especially
 * needed for UDF call expressions so the return type can be inferred by accessing a UdfDescriptor
 * via the UdfCatalog.
 */
class TypeInferencePhaseContext {
  public:
    TypeInferencePhaseContext(Catalogs::Source::SourceCatalogPtr sourceCatalog, Catalogs::UDF::UdfCatalogPtr udfCatalog);

    /**
     * Retrieve the source catalog
     * @return pointer to the source catalog
     */
    [[nodiscard]] const Catalogs::Source::SourceCatalogPtr& getSourceCatalog() const;

    /**
     * Return the UdfCatalog that is used for type inference
     * @return pointer to the udf catalog
     */
    [[nodiscard]] const Catalogs::UDF::UdfCatalogPtr& getUdfCatalog() const;

  private:
    const Catalogs::Source::SourceCatalogPtr sourceCatalog;
    const Catalogs::UDF::UdfCatalogPtr udfCatalog;
};

}// namespace NES::Optimizer
#endif// NES_CORE_INCLUDE_OPTIMIZER_PHASES_TYPEINFERENCEPHASECONTEXT_HPP_
