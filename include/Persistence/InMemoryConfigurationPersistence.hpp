//
// Created by moritz on 18/07/2021.
//

#ifndef NES_INMEMORYCONFIGURATIONPERSISTENCE_H
#define NES_INMEMORYCONFIGURATIONPERSISTENCE_H
#include <Persistence/ConfigurationPersistence.hpp>
namespace NES {
class InMemoryConfigurationPersistence : public ConfigurationPersistence {
  public:
    InMemoryConfigurationPersistence();

    bool persistConfiguration(SourceConfigPtr sourceConfig) override;

    std::vector<SourceConfigPtr> loadConfigurations() override;
};
}// namespace NES

#endif//NES_INMEMORYCONFIGURATIONPERSISTENCE_H
