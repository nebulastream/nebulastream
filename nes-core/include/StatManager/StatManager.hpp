//
// Created by moritz on 5/5/23.
//

#ifndef NES_STATMANAGER_H
#define NES_STATMANAGER_H

#include <API/Schema.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Runtime/MemoryLayout/DynamicTupleBuffer.hpp>
#include <Catalogs/Source/PhysicalSourceTypes/PhysicalSourceType.hpp>
#include <Common/Identifiers.hpp>
#include <Runtime/Execution/DataEmitter.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/Reconfigurable.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <Util/GatheringMode.hpp>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <optional>
#include <thread>

#include <vector>
#include "Sources/DataSource.hpp"


namespace NES {

  class StatManager {

    public:
      bool collectStat(DataSource* statDataSource);
      bool stopCollectingStat(DataSource* statDataSource);
      static void updateStats(SchemaPtr schema, Runtime::MemoryLayouts::DynamicTupleBuffer& dynBuffer);

  };


}

#endif//NES_STATMANAGER_H