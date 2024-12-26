/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_SenaryOPERATOR_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_SenaryOPERATOR_HPP_

#include <API/Schema.hpp>
#include <Operators/Operator.hpp>
#include <Operators/OperatorForwardDeclaration.hpp>

namespace NES {

/**
 * @brief A Senary operator with more the none input operator, thus it has a one, two, three, four, five and six input schema.
 */
class SenaryOperator : public virtual Operator {
  public:
    explicit SenaryOperator(OperatorId id);

    /**
   * @brief get the input schema of this operator from the one side
   * @return SchemaPtr
   */
    SchemaPtr getoneInputSchema() const;

    /**
    * @brief set the input schema of this operator for the one side
     * @param inputSchema
    */
    void setoneInputSchema(SchemaPtr inputSchema);

    /**
   * @brief get the input schema of this operator from the two
   * @return SchemaPtr
   */
    SchemaPtr gettwoInputSchema() const;

    /**
    * @brief set the input schema of this operator for the two
     * @param inputSchema
    */
    void settwoInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the input schema of this operator from the three side
    * @return SchemaPtr
    */
    SchemaPtr getthreeInputSchema() const;

    /**
     * @brief set the input schema of this operator for the three side
     * @param inputSchema
    */
    void setthreeInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the input schema of this operator from the four side
    * @return SchemaPtr
    */
    SchemaPtr getfourInputSchema() const;

    /**
     * @brief set the input schema of this operator for the four side
     * @param inputSchema
    */
    void setfourInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the input schema of this operator from the five side
    * @return SchemaPtr
    */
    SchemaPtr getfiveInputSchema() const;

    /**
     * @brief set the input schema of this operator for the five side
     * @param inputSchema
    */

    void setfiveInputSchema(SchemaPtr inputSchema);

    /**
    * @brief get the input schema of this operator from the six side
    * @return SchemaPtr
    */
    SchemaPtr getsixInputSchema() const;

    /**
     * @brief set the input schema of this operator for the six side
     * @param inputSchema
    */

    void setsixInputSchema(SchemaPtr inputSchema);


    /**
    * @brief get the result schema of this operator
    * @return SchemaPtr
    */
    SchemaPtr getOutputSchema() const override;

    /**
     * @brief set the result schema of this operator
     * @param outputSchema
    */
    void setOutputSchema(SchemaPtr outputSchema) override;

    /**
     * @brief Set the input origin ids for the one input stream.
     * @param originIds
     */
    void setoneInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the one input stream
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getoneInputOriginIds();

    /**
     * @brief Set the input origin ids for the two input stream.
     * @param originIds
     */
    void settwoInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the two input stream
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> gettwoInputOriginIds();

    /**
     * @brief Set the input origin ids for the three input stream.
     * @param originIds
     */

    void setthreeInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the three input stream
     * @return std::vector<OriginId>
     */

    virtual std::vector<OriginId> getthreeInputOriginIds();

    /**
     * @brief Set the input origin ids for the four input stream.
     * @param originIds
     */

    void setfourInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the four input stream
     * @return std::vector<OriginId>
     */

    virtual std::vector<OriginId> getfourInputOriginIds();

    /**
     * @brief Set the input origin ids for the five input stream.
     * @param originIds
     */

    void setfiveInputOriginIds(const std::vector<OriginId>& originIds);

    /**
     * @brief Gets the input origin ids for the five input stream
     * @return std::vector<OriginId>
     */

    virtual std::vector<OriginId> getfiveInputOriginIds();

    /**
     * @brief Set the input origin ids for the six input stream.
     * @param originIds
     */   

    void setsixInputOriginIds(const std::vector<OriginId>& originIds);


    /**
     * @brief Gets the input origin ids for the six input stream
     * @return std::vector<OriginId>
     */

    virtual std::vector<OriginId> getsixInputOriginIds();

    /**
     * @brief Gets the input origin from both sides
     * @return std::vector<OriginId>
     */
    virtual std::vector<OriginId> getAllInputOriginIds();

    /**
     * @brief Gets the output origin ids for the result stream
     * @return std::vector<OriginId> originids
     */
    std::vector<OriginId> getOutputOriginIds() const override;

    /**
     * @brief returns the string representation of the class
     * @return the string representation of the class
     */
    std::string toString() const override;

  protected:
    SchemaPtr oneInputSchema = Schema::create();
    SchemaPtr twoInputSchema = Schema::create();
    SchemaPtr threeInputSchema = Schema::create();
    SchemaPtr fourInputSchema = Schema::create();
    SchemaPtr fiveInputSchema = Schema::create();
    SchemaPtr sixInputSchema = Schema::create();
    SchemaPtr outputSchema = Schema::create();
    std::vector<SchemaPtr> distinctSchemas;
    std::vector<OriginId> oneInputOriginIds;
    std::vector<OriginId> twoInputOriginIds;
    std::vector<OriginId> threeInputOriginIds;
    std::vector<OriginId> fourInputOriginIds;
    std::vector<OriginId> fiveInputOriginIds;
    std::vector<OriginId> sixInputOriginIds;
};

}// namespace NES

#endif// NES_OPERATORS_INCLUDE_OPERATORS_ABSTRACTOPERATORS_ARITY_SenaryOPERATOR_HPP_
