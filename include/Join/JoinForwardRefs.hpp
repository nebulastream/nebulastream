#ifndef NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
#define NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
#include <memory>

namespace NES {
namespace Join {
class LogicalJoinDefinition;
typedef std::shared_ptr<LogicalJoinDefinition> LogicalJoinDefinitionPtr;
}// namespace Join
}// namespace NES
#endif//NES_INCLUDE_JOIN_JOINFORWARDREFS_HPP_
