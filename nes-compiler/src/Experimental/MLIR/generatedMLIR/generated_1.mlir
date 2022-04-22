module  {
  llvm.func @printFromMLIR(i8, ...) -> i32
  llvm.mlir.global external @IB() : !llvm.array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>
  llvm.mlir.global external @OB() : !llvm.array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>
  llvm.func @getNumTuples(...) -> i64
  llvm.mlir.global external @IBPtr() : !llvm.ptr<i8>
  llvm.func @printValueFromMLIR(...)
  func @execute(%arg0: i32) -> i32 attributes {llvm.emit_c_interface} {
    %0 = llvm.mlir.constant(0 : index) : i32
    %c0 = arith.constant 0 : index
    %c3 = arith.constant 3 : index
    %c1 = arith.constant 1 : index
    %c0_i32 = arith.constant 0 : i32
    // Main For Loop
    %2 = scf.for %arg1 = %c0 to %c3 step %c1 iter_args(%arg2 = %c0_i32) -> (i32) {
      %3 = llvm.mlir.addressof @IBPtr : !llvm.ptr<ptr<i8>>
      %4 = llvm.call @getNumTuples(%3) : (!llvm.ptr<ptr<i8>>) -> i64
      llvm.call @printValueFromMLIR(%4) : (i64) -> ()
      %5 = llvm.mlir.addressof @IB : !llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>
      %6 = llvm.mlir.addressof @OB : !llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>
      %7 = llvm.mlir.constant(0 : index) : i32
      %8 = llvm.getelementptr %5[%0, %arg2, %7] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i8>
      %9 = llvm.getelementptr %6[%0, %arg2, %7] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i8>
      %10 = llvm.load %8 : !llvm.ptr<i8>
      llvm.store %10, %9 : !llvm.ptr<i8>
      %11 = llvm.mlir.constant(1 : index) : i8
      %12 = llvm.call @printFromMLIR(%11, %8) : (i8, !llvm.ptr<i8>) -> i32
      %13 = llvm.mlir.constant(1 : index) : i32
      %14 = llvm.getelementptr %5[%0, %arg2, %13] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i32>
      %15 = llvm.getelementptr %6[%0, %arg2, %13] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i32>
      %16 = llvm.load %14 : !llvm.ptr<i32>
      llvm.store %16, %15 : !llvm.ptr<i32>
      %17 = llvm.mlir.constant(3 : index) : i8
      %18 = llvm.call @printFromMLIR(%17, %14) : (i8, !llvm.ptr<i32>) -> i32
      %19 = llvm.mlir.constant(2 : index) : i32
      %20 = llvm.getelementptr %5[%0, %arg2, %19] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i1>
      %21 = llvm.getelementptr %6[%0, %arg2, %19] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i1>
      %22 = llvm.load %20 : !llvm.ptr<i1>
      llvm.store %22, %21 : !llvm.ptr<i1>
      %23 = llvm.mlir.constant(7 : index) : i8
      %24 = llvm.call @printFromMLIR(%23, %20) : (i8, !llvm.ptr<i1>) -> i32
      %25 = llvm.mlir.constant(3 : index) : i32
      %26 = llvm.getelementptr %5[%0, %arg2, %25] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<f32>
      %27 = llvm.getelementptr %6[%0, %arg2, %25] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<f32>
      %28 = llvm.load %26 : !llvm.ptr<f32>
      llvm.store %28, %27 : !llvm.ptr<f32>
      %29 = llvm.mlir.constant(5 : index) : i8
      %30 = llvm.call @printFromMLIR(%29, %26) : (i8, !llvm.ptr<f32>) -> i32
      %31 = llvm.mlir.constant(4 : index) : i32
      %32 = llvm.getelementptr %5[%0, %arg2, %31] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i8>
      %33 = llvm.getelementptr %6[%0, %arg2, %31] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<i8>
      %34 = llvm.load %32 : !llvm.ptr<i8>
      llvm.store %34, %33 : !llvm.ptr<i8>
      %35 = llvm.mlir.constant(8 : index) : i8
      %36 = llvm.call @printFromMLIR(%35, %32) : (i8, !llvm.ptr<i8>) -> i32
      %37 = llvm.mlir.constant(5 : index) : i32
      %38 = llvm.getelementptr %5[%0, %arg2, %37] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<f64>
      %39 = llvm.getelementptr %6[%0, %arg2, %37] : (!llvm.ptr<array<6 x struct<packed (i8, i32, i1, f32, i8, f64)>>>, i32, i32, i32) -> !llvm.ptr<f64>
      %40 = llvm.load %38 : !llvm.ptr<f64>
      llvm.store %40, %39 : !llvm.ptr<f64>
      %41 = llvm.mlir.constant(6 : index) : i8
      %42 = llvm.call @printFromMLIR(%41, %38) : (i8, !llvm.ptr<f64>) -> i32
      %c42_i32 = arith.constant 42 : i32
      %43 = arith.cmpi slt, %arg2, %c42_i32 : i32
      %44 = scf.if %43 -> (i32) {
        %c1_i32 = arith.constant 1 : i32
        %45 = llvm.add %c1_i32, %arg2  : i32
        scf.yield %45 : i32
      } else {
        %c0_i32_0 = arith.constant 0 : i32
        %45 = llvm.add %c0_i32_0, %arg2  : i32
        scf.yield %45 : i32
      }
      scf.yield %44 : i32
    }
    llvm.return %2 : i32
  }
}
