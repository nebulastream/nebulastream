#loc2 = loc("-":2:17)
module  {
  func @execute(%arg0: i32 -:2:17) -> i32 attributes {llvm.emit_c_interface} {
    %c3_i32 = arith.constant 3 : i32 -:3:15
    %c0 = arith.constant 0 : index -:4:11
    %c5 = arith.constant 5 : index -:5:11
    %c1 = arith.constant 1 : index -:6:11
    %0 = scf.for %arg1 = %c0 to %c5 step %c1 iter_args(%arg2 = %c3_i32) -> (i32) {
      scf.yield %arg2 : i32 -:9:7
    } -:7:12
    return %c3_i32 : i32 -:11:5
  } -:2:3
} -:0:0
#loc0 = loc("-":0:0)
#loc1 = loc("-":2:3)
#loc3 = loc("-":3:15)
#loc4 = loc("-":4:11)
#loc5 = loc("-":5:11)
#loc6 = loc("-":6:11)
#loc7 = loc("-":7:12)
#loc8 = loc("-":9:7)
#loc9 = loc("-":11:5)
