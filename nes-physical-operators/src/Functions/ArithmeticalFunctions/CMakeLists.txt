# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(Abs PhysicalFunction AbsolutePhysicalFunction.cpp)
add_plugin(Add PhysicalFunction AddPhysicalFunction.cpp)
add_plugin(Ceil PhysicalFunction CeilPhysicalFunction.cpp)
add_plugin(Div PhysicalFunction DivPhysicalFunction.cpp)
add_plugin(Floor PhysicalFunction FloorPhysicalFunction.cpp)
add_plugin(Mod PhysicalFunction ModPhysicalFunction.cpp)
add_plugin(Mul PhysicalFunction MulPhysicalFunction.cpp)
add_plugin(Pow PhysicalFunction PowPhysicalFunction.cpp)
add_plugin(Round PhysicalFunction RoundPhysicalFunction.cpp)
add_plugin(Sub PhysicalFunction SubPhysicalFunction.cpp)
add_plugin(Sqrt PhysicalFunction SqrtPhysicalFunction.cpp)
