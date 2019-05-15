/**
 * UserAPIExpression class file
 * 
 * */

#include <string>
#include <sstream>

#include <CodeGen/CodeGen.hpp>
#include <CodeGen/C_CodeGen/BinaryOperatorStatement.hpp>
#include <CodeGen/C_CodeGen/Declaration.hpp>
#include <CodeGen/C_CodeGen/FileBuilder.hpp>
#include <CodeGen/C_CodeGen/FunctionBuilder.hpp>
#include <CodeGen/C_CodeGen/Statement.hpp>
#include <CodeGen/C_CodeGen/UnaryOperatorStatement.hpp>

#include <API/UserAPIExpression.hpp>

namespace iotdb
{
	
	Predicate::Predicate(const BinaryOperatorType& op, const UserAPIExpressionPtr left, const UserAPIExpressionPtr right, bool bracket = false) :
		_op(op),
		_left(left),
		_right(right),
		_bracket(bracket)
	{}
	
	UserAPIExpressionPtr Predicate::copy() const{
		return std::make_shared<Predicate>(*this);
	}

    const ExpressionStatmentPtr Predicate::generateCode(GeneratedCode& code) const{
		//toDo: implement code-generation
		//BinaryOperatorStatement bin_op(VarRefStatement(var_decl_i), PLUS_OP, VarRefStatement(var_decl_j));

		return BinaryOperatorStatement(*(_left->generateCode(code)), _op, *(_right->generateCode(code))).copy();

	}



    const ExpressionStatmentPtr PredicateItem::generateCode(GeneratedCode& code) const{
		//toDo: implement code-generation


		if(_attribute){

		    //toDo: Need an equals operator instead of true
		    if(code.struct_decl_input_tuple.getField(_attribute->name) &&
		            code.struct_decl_input_tuple.getField(_attribute->name)->getType() == _attribute->getDataType()){

                      VariableDeclaration var_decl_attr = code.struct_decl_input_tuple.getVariableDeclaration(_attribute->name);
                      return ((VarRef(code.var_decl_input_tuple)[VarRef(*code.var_decl_id)]).accessRef(VarRef(var_decl_attr))).copy();

//                return  VarRefStatement(
//                        VariableDeclaration::create(code.struct_decl_input_tuple.getField(_attribute->name)->getType(),
//                                                    code.struct_decl_input_tuple.getField(_attribute->name)->getIdentifierName())
//                        ).copy();
		      } else{
			IOTDB_FATAL_ERROR("Could not Retrieve Attribute from StructDeclaration!");
		      }
		}else if(_value){
		    return ConstantExprStatement(_value).copy();
		  }else{
		    IOTDB_FATAL_ERROR("PredicateItem has only NULL Pointers!");
		  }
	}
	
	const std::string Predicate::toString() const{
			std::stringstream stream;
			if(_bracket) stream << "(";
			stream << _left->toString() << " " << ::iotdb::toCodeExpression(_op)->code_ << " " << _right->toString() << " ";
			if(_bracket) stream << ")";
			return stream.str();
	}
	
	PredicateItem::PredicateItem(AttributeFieldPtr attribute) : 
	_mutation(PredicateItemMutation::ATTRIBUTE),
	_attribute(attribute)
	{}
	
	PredicateItem::PredicateItem(ValueTypePtr value) : 
	_mutation(PredicateItemMutation::VALUE),
	_value(value)
	{}
	
	const std::string PredicateItem::toString() const{
			switch(_mutation)
			{
				case PredicateItemMutation::ATTRIBUTE:
					return _attribute->toString();
				case PredicateItemMutation::VALUE:
					return _value->getCodeExpression()->code_;
			}
	}

	UserAPIExpressionPtr PredicateItem::copy() const{
		return std::make_shared<PredicateItem>(*this);
	}


	//create UserAPIExpression depending on the input value
    template<>
    UserAPIExpressionPtr getNeededExpression<bool>( bool value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::BOOLEAN, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<char>( char value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::CHAR, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<int8_t>( int8_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::INT8, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<uint8_t>( uint8_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::UINT8, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<int16_t>( int16_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::INT16, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<uint16_t>( uint16_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::UINT16, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<int32_t>( int32_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::INT32, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<uint32_t >( uint32_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::UINT32, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<int64_t >( int64_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::INT64, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<uint64_t >( uint64_t value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::UINT64, streamer.str())));
    }
    template<>
    UserAPIExpressionPtr getNeededExpression<float>( float value ){
	    std::stringstream streamer;
	    streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::FLOAT32, streamer.str())));
	}
    template<>
    UserAPIExpressionPtr getNeededExpression<double>( double value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::FLOAT64, streamer.str())));
    }

    // What about the Date-Expression? How to get the difference between uint 32? Not needed maybe?
    /*
	template<>
    UserAPIExpressionPtr getNeededExpression<uint32_t>( int value ){
        std::stringstream streamer;
        streamer << value;
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::INT32, streamer.str())));
    }
    */

    template<>
    UserAPIExpressionPtr getNeededExpression<void>( void ){
        return std::make_shared<PredicateItem>(PredicateItem(createBasicTypeValue(BasicType::VOID_TYPE, "")));
    }

    template<>
    UserAPIExpressionPtr getNeededExpression<UserAPIExpressionPtr>( UserAPIExpressionPtr value ){
        return value;
    }

    //Define operator overloading
    /*
    template<class T>
	Predicate operator == (const UserAPIExpression &lhs, const T& rhs){
		return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), getNeededExpression<T>(rhs));
	}
     */
    Predicate operator == (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
        return Predicate(BinaryOperatorType::EQUAL_OP, lhs.copy(), rhs.copy());
    }
	Predicate operator != (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::UNEQUAL_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator > (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::GREATER_THEN_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator < (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LESS_THEN_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator >= (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::GREATER_THEN_OP , lhs.copy(), rhs.copy());
	}
	Predicate operator <= (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LESS_THEN_EQUAL_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator + (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::PLUS_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator - (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::MINUS_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator * (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::MULTIPLY_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator / (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::DIVISION_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator % (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::MODULO_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator && (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LOGICAL_AND_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator || (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::LOGICAL_OR_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator & (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_AND_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator | (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_OR_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator ^ (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_XOR_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator << (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_LEFT_SHIFT_OP, lhs.copy(), rhs.copy());
	}
	Predicate operator >> (const UserAPIExpression &lhs, const UserAPIExpression &rhs){
		return Predicate(BinaryOperatorType::BITWISE_RIGHT_SHIFT_OP, lhs.copy(), rhs.copy());
	}
	
} //end namespace iotdb
