//
// Created by moritz on 5/5/23.
//

#include "StatManager/StatManager.hpp"

namespace NES {

  bool StatManager::collectStat(DataSource* statDataSource) {

    if (statDataSource->isRunning()) {
      NES_DEBUG2("DataSource {} is running", statDataSource->getOperatorId());

      // if the source is not yet collecting one or more stats
      if (!statDataSource->getCollectingStat()){
        statDataSource->setCollectingStat(true);
        statDataSource->setActiveStats(statDataSource->getActiveStats()+1);
      }
      return true;
    } else {
      NES_WARNING2("DataSource is not running");
      return false;
    }
  }

  bool StatManager::stopCollectingStat(DataSource* statDataSource) {

    if (!statDataSource->isRunning()) {
      NES_WARNING2("DataSource is not running. Nothing to stop.");
      return false;
    }

    return true;
  }

  void StatManager::updateStats(SchemaPtr schema, Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer) {

    for (Runtime::MemoryLayouts::DynamicTuple dynTuple : dynBuffer) {
      dynTuple.toString(schema);
    }

    return;
  }

}