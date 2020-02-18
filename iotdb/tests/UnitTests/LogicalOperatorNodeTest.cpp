#include <cassert>
#include <iostream>
#include <OperatorNodes/LogicalOperatorPlan.hpp>


#include <QueryCompiler/HandCodedQueryExecutionPlan.hpp>
#include <NodeEngine/Dispatcher.hpp>

#include <API/InputQuery.hpp>
#include <API/UserAPIExpression.hpp>
#include <API/Environment.hpp>
#include <API/Types/DataTypes.hpp>
#include <Util/UtilityFunctions.hpp>
#include <Operators/Operator.hpp>

#include <API/InputQuery.hpp>
#include <API/Config.hpp>
#include <API/Schema.hpp>
#include <SourceSink/DataSource.hpp>
#include <API/InputQuery.hpp>
#include <API/Environment.hpp>
#include <API/UserAPIExpression.hpp>
#include <Catalogs/StreamCatalog.hpp>


namespace NES {

    void createQueryString() {
        StreamPtr sPtr = StreamCatalog::instance().getStreamForLogicalStreamOrThrowException("default_logical");
        InputQuery inputQuery = InputQuery::from(*sPtr.get())
            .filter((*sPtr.get())["value"]!=5)
            // .filter((*sPtr.get())["value"]!=6)
            .print(std::cout);
        // .filter((*sPtr.get())["value"]!=6)
        // .map(AttributeField("value"), (*sPtr.get())["value"]!=5);
        // std::cout << inputQuery.getRoot()->toString() << std::endl;
        // std::cout << inputQuery.getRoot()->getOperatorType() << std::endl;
        // auto children = inputQuery.getRoot()->getChildren();
        // for (auto&& child : children) {
        //     std::cout << child->toString() << std::endl;
        //     auto _children = child->getChildren();
        //     if (_children.empty()) {

        //     } else {
        //         std::cout << "have children" << std::endl;
        //     }
        // }
        InputQueryPtr inputQueryPtr = std::make_shared<InputQuery>(inputQuery);
        LogicalOperatorPlan logicalOperatorPlan(inputQueryPtr);
        std::cout << "================================================================================" << std::endl;
        logicalOperatorPlan.prettyPrint();
    }
    void createLogicalPlanTree1() {
        StreamPtr sPtr = StreamCatalog::instance().getStreamForLogicalStreamOrThrowException("default_logical");
        LogicalOperatorPlan query = LogicalOperatorPlan::from(*sPtr.get())
            .filter((*sPtr.get())["value"]!=5)
            .print(std::cout);
        std::cout << "================================================================================" << std::endl;
        query.prettyPrint();

    }
    void createLogicalPlanTree2() {
        StreamPtr sPtr = StreamCatalog::instance().getStreamForLogicalStreamOrThrowException("default_logical");
        LogicalOperatorPlan query = LogicalOperatorPlan::from(*sPtr.get());
        // query = query.filter((*sPtr.get())["value"]!=5);
        // query = query.filter((*sPtr.get())["value"]!=6);
        query = query.print(std::cout);
        std::cout << "================================================================================" << std::endl;
        query.prettyPrint();
        std::cout << "================================================================================" << std::endl;
        auto x = query.getOperatorNodeById("invalid-id");
        auto y = query.getOperatorNodeById(query.getRoot()->getOperatorId());
        if (x == nullptr) {
            std::cout << "correct" << std::endl;
        }
        if (y == query.getRoot()) {
            std::cout << "correct" << std::endl;
        }

    }

} // namespace NES

int main(int argc, const char *argv[]) {

    NES::Dispatcher::instance();
    NES::createQueryString();
    NES::createLogicalPlanTree1();
    NES::createLogicalPlanTree2();
    return 0;
}
