#ifndef NES_GLOBALQUERYOPERATOR_HPP
#define NES_GLOBALQUERYOPERATOR_HPP

#include <string>
#include <memory>
#include <vector>

namespace NES {

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class GlobalQueryOperator;
typedef std::shared_ptr<GlobalQueryOperator> GlobalQueryOperatorPtr;

class GlobalQueryOperator : public Node {
    static GlobalQueryOperatorPtr create();

    /**
     * @brief add new query to the
     * @param queryId
     */
    void addQuery(std::string queryId);

    /**
     * @brief
     * @param queryId
     */
    void removeQuery(std::string queryId);

  private:
    GlobalQueryOperator();

    std::vector<std::string> queryIds
    OperatorNodePtr logicalOperator;
};
}
#endif//NES_GLOBALQUERYOPERATOR_HPP
