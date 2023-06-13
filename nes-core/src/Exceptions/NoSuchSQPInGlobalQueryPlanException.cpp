#include <Exceptions/NoSuchSQPInGlobalQueryPlanException.hpp>
#include <utility>
namespace NES::Exceptions {
NoSuchSQPInGlobalQueryPlanException::NoSuchSQPInGlobalQueryPlanException(std::string message,
                                        NES::SharedQueryId id) : message(std::move(message)), id(id) {}
}
