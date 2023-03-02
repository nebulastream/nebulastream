#include <TestUtils/RecordCollectOperator.hpp>
namespace NES::Runtime::Execution::Operators {

void CollectOperator::execute(ExecutionContext&, Record& record) const { records.emplace_back(record); }

}// namespace NES::Runtime::Execution::Operators