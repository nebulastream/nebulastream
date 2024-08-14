# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

 add_source_files(nes-execution
         AbsExpression.cpp
         AcosExpression.cpp
         Log2Expression.cpp
         Log10Expression.cpp
         LnExpression.cpp

         # We disable the other expressions as we have not ported them yet
         #         ModExpression.cpp
         #         PiExpression.cpp
         #         RadiansExpression.cpp
         #         DegreesExpression.cpp
         #         CosExpression.cpp
         #         SinExpression.cpp
         #         CotExpression.cpp
         #         AsinExpression.cpp
         #         MinExpression.cpp
         #         MaxExpression.cpp
         #         AtanExpression.cpp
         #         Atan2Expression.cpp
         #         SqrtExpression.cpp
         #         CbrtExpression.cpp
         #         FactorialExpression.cpp
         #         GammaExpression.cpp
         #         LGammaExpression.cpp
         #         PowerExpression.cpp
         #         BitcounterExpression.cpp
#         TanExpression.cpp
#         FloorExpression.cpp
#         CeilExpression.cpp
#         RandomExpression.cpp
#         TruncExpression.cpp
#         SignExpression.cpp
#         ExpExpression.cpp
         )