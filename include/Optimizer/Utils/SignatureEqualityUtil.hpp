/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_SIGNATUREEQUALITYUTIL_HPP
#define NES_SIGNATUREEQUALITYUTIL_HPP

#include <memory>

namespace z3 {
class solver;
typedef std::shared_ptr<solver> SolverPtr;

class context;
typedef std::shared_ptr<context> ContextPtr;
}// namespace z3

namespace NES::Optimizer {

class QuerySignature;
typedef std::shared_ptr<QuerySignature> QuerySignaturePtr;

class SignatureEqualityUtil;
typedef std::shared_ptr<SignatureEqualityUtil> SignatureEqualityUtilPtr;

class SignatureEqualityUtil {

  public:
    static SignatureEqualityUtilPtr create(z3::ContextPtr context);

    SignatureEqualityUtil(z3::ContextPtr context);

    bool checkEquality(QuerySignaturePtr signature1, QuerySignaturePtr signature2);

  private:
    z3::ContextPtr context;
    z3::SolverPtr solver;
};
}// namespace NES::Optimizer
#endif//NES_SIGNATUREEQUALITYUTIL_HPP
