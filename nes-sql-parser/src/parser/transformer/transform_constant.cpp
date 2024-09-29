//
// Created by Usama Bin Tariq on 23.09.24.
//

#include <nebula/parser/transformer/transformer.hpp>
#include <nebula/parser/expressions/constant_expression.hpp>
#include <string>
#include <vector>
#include <memory>

namespace nebula {
    std::unique_ptr<ParsedExpression> Transformer::TransformConstant(pgquery::PGAConst &con) {
        const auto val = con.val;
        switch (val.type) {
            case pgquery::T_PGInteger:
                return std::make_unique<ConstantExpression>(Value((int32_t) val.val.ival));
            case pgquery::T_PGString:
            case pgquery::T_PGBitString: {
                return std::make_unique<ConstantExpression>(Value((std::string(val.val.str))));
            }
            case pgquery::T_PGNull: return std::make_unique<ConstantExpression>(Value());
            case pgquery::T_PGFloat: {
                //FIXME: need to parse exponent
                std::string str = val.val.str;
                auto f = std::stof(str);
                return std::make_unique<ConstantExpression>(Value(f));
            }
            default:
                return nullptr;
        }
    }
}
