//
// Created by balint on 15.01.22.
//

#ifndef NES_MIGRATIONTYPES_HPP
#define NES_MIGRATIONTYPES_HPP
#include <vector>
#include <cstdint>
namespace NES {

/**
 * @brief these alias represent Migration Types
 */
using MigrationType = uint8_t;
static constexpr uint8_t RESTART = 1;
static constexpr uint8_t MIGRATION_WITH_BUFFERING = 2;
static constexpr uint8_t MIGRATION_WITHOUT_BUFFERING = 3;
inline static const std::vector<MigrationType> migrationTypes = {RESTART, MIGRATION_WITH_BUFFERING, MIGRATION_WITHOUT_BUFFERING};
}
#endif//NES_MIGRATIONTYPES_HPP
