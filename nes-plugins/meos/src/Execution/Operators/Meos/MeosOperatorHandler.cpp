
// #include <Execution/Operators/MEOS/MeosOperatorHandler.hpp>
// #include <Runtime/WorkerContext.hpp>

// namespace NES::Runtime::Execution::Operators {

// MeosOperatorHandler::MeosOperatorHandler(const std::string& function)
//     : function(function) {
// }

// MeosOperatorHandlerPtr MeosOperatorHandler::create(const std::string& function) {
//     return std::make_shared<MeosOperatorHandler>(function);
// }

// void MeosOperatorHandler::start(Runtime::Execution::PipelineExecutionContextPtr, uint32_t) {}

// void MeosOperatorHandler::stop(Runtime::QueryTerminationType, Runtime::Execution::PipelineExecutionContextPtr) {}

// void MeosOperatorHandler::reconfigure(Runtime::ReconfigurationMessage&, Runtime::WorkerContext&) {}

// void MeosOperatorHandler::postReconfigurationCallback(Runtime::ReconfigurationMessage&) {}

// const std::string& MeosOperatorHandler::getFunction() const { return function; }

// const Expressions::MeosOperator& MeosOperatorHandler::getMeosAdapter() const { return meosAdapter; }

// }// namespace NES::Runtime::Execution::Operators
