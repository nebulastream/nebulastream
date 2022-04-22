module {
  llvm.func @printf(!llvm.ptr<i8>, ...) -> i32
  llvm.mlir.global internal constant @nl("\0A\00")
  llvm.mlir.global external @TB() : !llvm.array<2 x i32>
  func @execute(%arg0: i32) -> i32 attributes {llvm.emit_c_interface} {
    %c100 = arith.constant 100 : index
    %c0 = arith.constant 0 : index
    %c1 = arith.constant 1 : index
    %c0_i32 = arith.constant 0 : i32
    %0 = scf.for %arg1 = %c0 to %c100 step %c1 iter_args(%arg2 = %c0_i32) -> (i32) {
      %c42_i32 = arith.constant 42 : i32
      %1 = arith.cmpi slt, %arg2, %c42_i32 : i32
      %2 = scf.if %1 -> (i32) {
        %c1_i32 = arith.constant 1 : i32
        %3 = llvm.add %c1_i32, %arg2  : i32
        %4 = llvm.mlir.addressof @nl : !llvm.ptr<array<2 x i8>>
        %5 = llvm.mlir.addressof @TB : !llvm.ptr<array<2 x i32>>
        %6 = llvm.mlir.constant(0 : index) : i64
        %7 = llvm.getelementptr %4[%6, %6] : (!llvm.ptr<array<2 x i8>>, i64, i64) -> !llvm.ptr<i8>
        %8 = llvm.getelementptr %5[%6, %6] : (!llvm.ptr<array<2 x i32>>, i64, i64) -> !llvm.ptr<i32>
        %9 = llvm.call @printf(%7) : (!llvm.ptr<i8>) -> i32
        %10 = llvm.load %8 : !llvm.ptr<i32>
        scf.yield %10 : i32
      } else {
        %c0_i32_0 = arith.constant 0 : i32
        %3 = llvm.add %c0_i32_0, %arg2  : i32
        %4 = llvm.mlir.addressof @nl : !llvm.ptr<array<2 x i8>>
        %5 = llvm.mlir.constant(0 : index) : i64
        %6 = llvm.getelementptr %4[%5, %5] : (!llvm.ptr<array<2 x i8>>, i64, i64) -> !llvm.ptr<i8>
        %7 = llvm.call @printf(%6) : (!llvm.ptr<i8>) -> i32
        scf.yield %3 : i32
      }
      scf.yield %2 : i32
    }
    llvm.return %0 : i32
  }
}
