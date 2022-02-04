#ifndef NES_SYSTEMRESOURCESREADERFACTORY_HPP
#define NES_SYSTEMRESOURCESREADERFACTORY_HPP
#include <memory>

namespace NES {

class AbstractSystemResourcesReader;
using AbstractSystemResourcesReaderPtr = std::shared_ptr<AbstractSystemResourcesReader>;

class SystemsReaderFactory {
  public:
    /**
     * @brief Creates the appropriate SystemResourcesReader for the OS
     * @return the SystemResourcesReader
     */
    static AbstractSystemResourcesReaderPtr getSystemResourcesReader();
};

}

#endif//NES_SYSTEMRESOURCESREADERFACTORY_HPP
