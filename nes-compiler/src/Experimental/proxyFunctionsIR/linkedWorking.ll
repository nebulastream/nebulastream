; ModuleID = 'LLVMDialectModule'
source_filename = "LLVMDialectModule"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-unknown-linux-gnu"

%"class.NES::Runtime::detail::MemorySegment" = type { i8*, i32, %"class.NES::Runtime::detail::BufferControlBlock"* }
%"class.NES::Runtime::detail::BufferControlBlock" = type { %"struct.std::atomic", i32, i64, i64, i64, i64, %"class.NES::Runtime::detail::MemorySegment"*, %"struct.std::atomic.0", %"class.std::function" }
%"struct.std::atomic" = type { %"struct.std::__atomic_base" }
%"struct.std::__atomic_base" = type { i32 }
%"struct.std::atomic.0" = type { %"struct.std::__atomic_base.1" }
%"struct.std::__atomic_base.1" = type { %"class.NES::Runtime::BufferRecycler"* }
%"class.NES::Runtime::BufferRecycler" = type opaque
%"class.std::function" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* }
%"class.std::_Function_base" = type { %"union.std::_Any_data", i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* }
%"union.std::_Any_data" = type { %"union.std::_Nocopy_types" }
%"union.std::_Nocopy_types" = type { { i64, i64 } }
%"class.std::__cxx11::basic_string" = type { %"struct.std::__cxx11::basic_string<char>::_Alloc_hider", i64, %union.anon }
%"struct.std::__cxx11::basic_string<char>::_Alloc_hider" = type { i8* }
%union.anon = type { i64, [8 x i8] }
%"class.std::__cxx11::basic_stringbuf" = type { %"class.std::basic_streambuf", i32, %"class.std::__cxx11::basic_string" }
%"class.std::basic_streambuf" = type { i32 (...)**, i8*, i8*, i8*, i8*, i8*, i8*, %"class.std::locale" }
%"class.std::locale" = type { %"class.std::locale::_Impl"* }
%"class.std::locale::_Impl" = type { i32, %"class.std::locale::facet"**, i64, %"class.std::locale::facet"**, i8** }
%"class.std::locale::facet" = type <{ i32 (...)**, i32, [4 x i8] }>
%"class.std::basic_ostream" = type { i32 (...)**, %"class.std::basic_ios" }
%"class.std::basic_ios" = type { %"class.std::ios_base", %"class.std::basic_ostream"*, i8, i8, %"class.std::basic_streambuf"*, %"class.std::ctype"*, %"class.std::num_put"*, %"class.std::num_put"* }
%"class.std::ios_base" = type { i32 (...)**, i64, i64, i32, i32, i32, %"struct.std::ios_base::_Callback_list"*, %"struct.std::ios_base::_Words", [8 x %"struct.std::ios_base::_Words"], i32, %"struct.std::ios_base::_Words"*, %"class.std::locale" }
%"struct.std::ios_base::_Callback_list" = type { %"struct.std::ios_base::_Callback_list"*, void (i32, %"class.std::ios_base"*, i32)*, i32, i32 }
%"struct.std::ios_base::_Words" = type { i8*, i64 }
%"class.std::ctype" = type <{ %"class.std::locale::facet.base", [4 x i8], %struct.__locale_struct*, i8, [7 x i8], i32*, i32*, i16*, i8, [256 x i8], [256 x i8], i8, [6 x i8] }>
%"class.std::locale::facet.base" = type <{ i32 (...)**, i32 }>
%struct.__locale_struct = type { [13 x %struct.__locale_data*], i16*, i32*, i32*, [13 x i8*] }
%struct.__locale_data = type opaque
%"class.std::num_put" = type { %"class.std::locale::facet.base", [4 x i8] }
%"struct.std::experimental::fundamentals_v2::source_location" = type { i8*, i8*, i32, i32 }
%"class.NES::Exceptions::RuntimeException" = type { %"class.std::exception", %"class.std::__cxx11::basic_string" }
%"class.std::exception" = type { i32 (...)** }
%class.anon = type { %"class.std::function", %"class.std::function" }
%"class.std::type_info" = type { i32 (...)**, i8* }

$_ZN3NES10Exceptions16RuntimeExceptionD1Ev = comdat any

$__clang_call_terminate = comdat any

@IB = external global [6 x <{ i8, i32, i1, float, i8, double }>]
@OB = external local_unnamed_addr global [6 x <{ i8, i32, i1, float, i8, double }>]
@IBPtr = external local_unnamed_addr global i8*
@_ZTVSt15basic_streambufIcSt11char_traitsIcEE = external dso_local unnamed_addr constant { [16 x i8*] }, align 8
@_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE = external dso_local unnamed_addr constant { [16 x i8*] }, align 8
@_ZTVSo = external dso_local unnamed_addr constant { [5 x i8*], [5 x i8*] }, align 8
@.str = private unnamed_addr constant [32 x i8] c"[MemorySegment] invalid pointer\00", align 1
@.str.1 = private unnamed_addr constant [20 x i8] c"TupleBufferImpl.cpp\00", align 1
@.str.2 = private unnamed_addr constant [14 x i8] c"MemorySegment\00", align 1
@_ZTIN3NES10Exceptions16RuntimeExceptionE = external dso_local constant i8*
@.str.3 = private unnamed_addr constant [29 x i8] c"[MemorySegment] invalid size\00", align 1
@_ZTTN3NES10Exceptions16RuntimeExceptionE = external unnamed_addr constant [2 x i8*], align 8
@.str.4 = private unnamed_addr constant [30 x i8] c"Failed assertion on this->ptr\00", align 1
@.str.5 = private unnamed_addr constant [17 x i8] c" error message: \00", align 1
@.str.6 = private unnamed_addr constant [12 x i8] c"invalid ptr\00", align 1
@.str.7 = private unnamed_addr constant [31 x i8] c"Failed assertion on this->size\00", align 1
@.str.8 = private unnamed_addr constant [13 x i8] c"invalid size\00", align 1
@.str.9 = private unnamed_addr constant [32 x i8] c"Failed assertion on refCnt == 0\00", align 1
@.str.10 = private unnamed_addr constant [42 x i8] c"[MemorySegment] invalid reference counter\00", align 1
@.str.11 = private unnamed_addr constant [21 x i8] c" on mem segment dtor\00", align 1
@.str.12 = private unnamed_addr constant [29 x i8] c"Failed assertion on recycler\00", align 1
@.str.13 = private unnamed_addr constant [17 x i8] c"invalid recycler\00", align 1
@.str.14 = private unnamed_addr constant [44 x i8] c"Failed assertion on recycler != oldRecycler\00", align 1
@"_ZTIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEEE3$_0" = internal constant { i8*, i8* } { i8* bitcast (i8** getelementptr inbounds (i8*, i8** @_ZTVN10__cxxabiv117__class_type_infoE, i64 2) to i8*), i8* getelementptr inbounds ([128 x i8], [128 x i8]* @"_ZTSZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEEE3$_0", i32 0, i32 0) }, align 8
@_ZTVN10__cxxabiv117__class_type_infoE = external dso_local global i8*
@"_ZTSZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEEE3$_0" = internal constant [128 x i8] c"ZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEEE3$_0\00", align 1
@.str.15 = private unnamed_addr constant [36 x i8] c"Failed assertion on prevRefCnt != 0\00", align 1
@.str.16 = private unnamed_addr constant [57 x i8] c"BufferControlBlock: releasing an already released buffer\00", align 1

@_ZN3NES7Runtime6detail13MemorySegmentC1ERKS2_ = dso_local unnamed_addr alias void (%"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"*), void (%"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"*)* @_ZN3NES7Runtime6detail13MemorySegmentC2ERKS2_
@_ZN3NES7Runtime6detail13MemorySegmentC1EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EE = dso_local unnamed_addr alias void (%"class.NES::Runtime::detail::MemorySegment"*, i8*, i32, %"class.NES::Runtime::BufferRecycler"*, %"class.std::function"*), void (%"class.NES::Runtime::detail::MemorySegment"*, i8*, i32, %"class.NES::Runtime::BufferRecycler"*, %"class.std::function"*)* @_ZN3NES7Runtime6detail13MemorySegmentC2EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EE
@_ZN3NES7Runtime6detail13MemorySegmentC1EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EEb = dso_local unnamed_addr alias void (%"class.NES::Runtime::detail::MemorySegment"*, i8*, i32, %"class.NES::Runtime::BufferRecycler"*, %"class.std::function"*, i1), void (%"class.NES::Runtime::detail::MemorySegment"*, i8*, i32, %"class.NES::Runtime::BufferRecycler"*, %"class.std::function"*, i1)* @_ZN3NES7Runtime6detail13MemorySegmentC2EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EEb
@_ZN3NES7Runtime6detail13MemorySegmentD1Ev = dso_local unnamed_addr alias void (%"class.NES::Runtime::detail::MemorySegment"*), void (%"class.NES::Runtime::detail::MemorySegment"*)* @_ZN3NES7Runtime6detail13MemorySegmentD2Ev
@_ZN3NES7Runtime6detail18BufferControlBlockC1EPNS1_13MemorySegmentEPNS0_14BufferRecyclerEOSt8functionIFvS4_S6_EE = dso_local unnamed_addr alias void (%"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::BufferRecycler"*, %"class.std::function"*), void (%"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::BufferRecycler"*, %"class.std::function"*)* @_ZN3NES7Runtime6detail18BufferControlBlockC2EPNS1_13MemorySegmentEPNS0_14BufferRecyclerEOSt8functionIFvS4_S6_EE
@_ZN3NES7Runtime6detail18BufferControlBlockC1ERKS2_ = dso_local unnamed_addr alias void (%"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"*), void (%"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"*)* @_ZN3NES7Runtime6detail18BufferControlBlockC2ERKS2_

declare i32 @printFromMLIR(i8 %0, ...) local_unnamed_addr

declare void @printValueFromMLIR(...) local_unnamed_addr

define i32 @execute(i32 %0) local_unnamed_addr !dbg !5 {
  %2 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** bitcast (i8** @IBPtr to %"class.NES::Runtime::detail::BufferControlBlock"**), align 8, !dbg !9, !tbaa !11
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %2, i64 0, i32 1, !dbg !9
  %4 = load i32, i32* %3, align 4, !dbg !9, !tbaa !17
  %5 = zext i32 %4 to i64, !dbg !9
  tail call void (...) @printValueFromMLIR(i64 %5), !dbg !24
  %6 = load i8, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 0), align 1, !dbg !25
  store i8 %6, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 0, i32 0), align 1, !dbg !26
  %7 = tail call i32 (i8, ...) @printFromMLIR(i8 1, i8* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 0)), !dbg !27
  %8 = load i32, i32* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 1), align 4, !dbg !28
  store i32 %8, i32* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 0, i32 1), align 4, !dbg !29
  %9 = tail call i32 (i8, ...) @printFromMLIR(i8 3, i32* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 1)), !dbg !30
  %10 = load i1, i1* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 2), align 1, !dbg !31
  store i1 %10, i1* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 0, i32 2), align 1, !dbg !32
  %11 = tail call i32 (i8, ...) @printFromMLIR(i8 7, i1* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 2)), !dbg !33
  %12 = load float, float* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 3), align 4, !dbg !34
  store float %12, float* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 0, i32 3), align 4, !dbg !35
  %13 = tail call i32 (i8, ...) @printFromMLIR(i8 5, float* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 3)), !dbg !36
  %14 = load i8, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 4), align 1, !dbg !37
  store i8 %14, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 0, i32 4), align 1, !dbg !38
  %15 = tail call i32 (i8, ...) @printFromMLIR(i8 8, i8* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 4)), !dbg !39
  %16 = load double, double* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 5), align 8, !dbg !40
  store double %16, double* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 0, i32 5), align 8, !dbg !41
  %17 = tail call i32 (i8, ...) @printFromMLIR(i8 6, double* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 0, i32 5)), !dbg !42
  %18 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** bitcast (i8** @IBPtr to %"class.NES::Runtime::detail::BufferControlBlock"**), align 8, !dbg !9, !tbaa !11
  %19 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %18, i64 0, i32 1, !dbg !9
  %20 = load i32, i32* %19, align 4, !dbg !9, !tbaa !17
  %21 = zext i32 %20 to i64, !dbg !9
  tail call void (...) @printValueFromMLIR(i64 %21), !dbg !24
  %22 = load i8, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 0), align 1, !dbg !25
  store i8 %22, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 1, i32 0), align 1, !dbg !26
  %23 = tail call i32 (i8, ...) @printFromMLIR(i8 1, i8* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 0)), !dbg !27
  %24 = load i32, i32* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 1), align 4, !dbg !28
  store i32 %24, i32* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 1, i32 1), align 4, !dbg !29
  %25 = tail call i32 (i8, ...) @printFromMLIR(i8 3, i32* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 1)), !dbg !30
  %26 = load i1, i1* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 2), align 1, !dbg !31
  store i1 %26, i1* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 1, i32 2), align 1, !dbg !32
  %27 = tail call i32 (i8, ...) @printFromMLIR(i8 7, i1* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 2)), !dbg !33
  %28 = load float, float* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 3), align 4, !dbg !34
  store float %28, float* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 1, i32 3), align 4, !dbg !35
  %29 = tail call i32 (i8, ...) @printFromMLIR(i8 5, float* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 3)), !dbg !36
  %30 = load i8, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 4), align 1, !dbg !37
  store i8 %30, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 1, i32 4), align 1, !dbg !38
  %31 = tail call i32 (i8, ...) @printFromMLIR(i8 8, i8* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 4)), !dbg !39
  %32 = load double, double* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 5), align 8, !dbg !40
  store double %32, double* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 1, i32 5), align 8, !dbg !41
  %33 = tail call i32 (i8, ...) @printFromMLIR(i8 6, double* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 1, i32 5)), !dbg !42
  %34 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** bitcast (i8** @IBPtr to %"class.NES::Runtime::detail::BufferControlBlock"**), align 8, !dbg !9, !tbaa !11
  %35 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %34, i64 0, i32 1, !dbg !9
  %36 = load i32, i32* %35, align 4, !dbg !9, !tbaa !17
  %37 = zext i32 %36 to i64, !dbg !9
  tail call void (...) @printValueFromMLIR(i64 %37), !dbg !24
  %38 = load i8, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 0), align 1, !dbg !25
  store i8 %38, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 2, i32 0), align 1, !dbg !26
  %39 = tail call i32 (i8, ...) @printFromMLIR(i8 1, i8* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 0)), !dbg !27
  %40 = load i32, i32* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 1), align 4, !dbg !28
  store i32 %40, i32* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 2, i32 1), align 4, !dbg !29
  %41 = tail call i32 (i8, ...) @printFromMLIR(i8 3, i32* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 1)), !dbg !30
  %42 = load i1, i1* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 2), align 1, !dbg !31
  store i1 %42, i1* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 2, i32 2), align 1, !dbg !32
  %43 = tail call i32 (i8, ...) @printFromMLIR(i8 7, i1* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 2)), !dbg !33
  %44 = load float, float* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 3), align 4, !dbg !34
  store float %44, float* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 2, i32 3), align 4, !dbg !35
  %45 = tail call i32 (i8, ...) @printFromMLIR(i8 5, float* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 3)), !dbg !36
  %46 = load i8, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 4), align 1, !dbg !37
  store i8 %46, i8* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 2, i32 4), align 1, !dbg !38
  %47 = tail call i32 (i8, ...) @printFromMLIR(i8 8, i8* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 4)), !dbg !39
  %48 = load double, double* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 5), align 8, !dbg !40
  store double %48, double* getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @OB, i64 0, i64 2, i32 5), align 8, !dbg !41
  %49 = tail call i32 (i8, ...) @printFromMLIR(i8 6, double* nonnull getelementptr inbounds ([6 x <{ i8, i32, i1, float, i8, double }>], [6 x <{ i8, i32, i1, float, i8, double }>]* @IB, i64 0, i64 2, i32 5)), !dbg !42
  ret i32 3, !dbg !43
}

define i32 @_mlir_ciface_execute(i32 %0) local_unnamed_addr !dbg !44 {
  %2 = tail call i32 @execute(i32 undef), !dbg !45
  ret i32 %2, !dbg !45
}

define void @_mlir_execute(i8** nocapture readonly %0) local_unnamed_addr {
  %2 = tail call i32 @execute(i32 undef)
  %3 = getelementptr i8*, i8** %0, i64 1
  %4 = bitcast i8** %3 to i32**
  %5 = load i32*, i32** %4, align 8
  store i32 %2, i32* %5, align 4
  ret void
}

define void @_mlir__mlir_ciface_execute(i8** nocapture readonly %0) local_unnamed_addr {
  %2 = tail call i32 @execute(i32 undef), !dbg !45
  %3 = getelementptr i8*, i8** %0, i64 1
  %4 = bitcast i8** %3 to i32**
  %5 = load i32*, i32** %4, align 8
  store i32 %2, i32* %5, align 4
  ret void
}

; Function Attrs: alwaysinline mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local i64 @getNumTuples(i8* nocapture readonly %0) local_unnamed_addr #0 {
  %2 = bitcast i8* %0 to %"class.NES::Runtime::detail::BufferControlBlock"**
  %3 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %2, align 8, !tbaa !11
  %4 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %3, i64 0, i32 1
  %5 = load i32, i32* %4, align 4, !tbaa !17
  %6 = zext i32 %5 to i64
  ret i64 %6
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock17getNumberOfTuplesEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #1 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  %3 = load i32, i32* %2, align 4, !tbaa !17
  %4 = zext i32 %3 to i64
  ret i64 %4
}

; Function Attrs: mustprogress nofree nosync nounwind uwtable willreturn
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentC2ERKS2_(%"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull dereferenceable(24) %0, %"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull readonly align 8 dereferenceable(24) %1) unnamed_addr #2 align 2 {
  %3 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %0 to i8*
  %4 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %1 to i8*
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(24) %3, i8* noundef nonnull align 8 dereferenceable(24) %4, i64 24, i1 false)
  ret void
}

; Function Attrs: argmemonly mustprogress nofree nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly %0, i8* noalias nocapture readonly %1, i64 %2, i1 immarg %3) #3

; Function Attrs: mustprogress nofree nosync nounwind uwtable willreturn
define dso_local nonnull align 8 dereferenceable(24) %"class.NES::Runtime::detail::MemorySegment"* @_ZN3NES7Runtime6detail13MemorySegmentaSERKS2_(%"class.NES::Runtime::detail::MemorySegment"* nonnull returned dereferenceable(24) %0, %"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull readonly align 8 dereferenceable(24) %1) local_unnamed_addr #2 align 2 {
  %3 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %0 to i8*
  %4 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %1 to i8*
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(24) %3, i8* noundef nonnull align 8 dereferenceable(24) %4, i64 24, i1 false)
  ret %"class.NES::Runtime::detail::MemorySegment"* %0
}

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentC2EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EE(%"class.NES::Runtime::detail::MemorySegment"* nonnull dereferenceable(24) %0, i8* %1, i32 %2, %"class.NES::Runtime::BufferRecycler"* %3, %"class.std::function"* nocapture nonnull align 8 dereferenceable(32) %4) unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %6 = alloca { i64, i64 }, align 8
  %7 = alloca %"class.std::__cxx11::basic_string", align 8
  %8 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %9 = alloca %"class.std::basic_ostream", align 8
  %10 = alloca %"class.std::__cxx11::basic_string", align 8
  %11 = alloca %"struct.std::experimental::fundamentals_v2::source_location", align 8
  %12 = alloca %"class.std::__cxx11::basic_string", align 8
  %13 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %14 = alloca %"class.std::basic_ostream", align 8
  %15 = alloca %"class.std::__cxx11::basic_string", align 8
  %16 = alloca %"struct.std::experimental::fundamentals_v2::source_location", align 8
  %17 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 0
  store i8* %1, i8** %17, align 8, !tbaa !47
  %18 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 1
  store i32 %2, i32* %18, align 8, !tbaa !49
  %19 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 2
  store %"class.NES::Runtime::detail::BufferControlBlock"* null, %"class.NES::Runtime::detail::BufferControlBlock"** %19, align 8, !tbaa !50
  %20 = zext i32 %2 to i64
  %21 = getelementptr inbounds i8, i8* %1, i64 %20
  %22 = getelementptr inbounds i8, i8* %21, i64 40
  %23 = bitcast i8* %22 to %"class.NES::Runtime::detail::MemorySegment"**
  tail call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 4 dereferenceable(40) %21, i8 0, i64 40, i1 false) #18
  store %"class.NES::Runtime::detail::MemorySegment"* %0, %"class.NES::Runtime::detail::MemorySegment"** %23, align 8, !tbaa !51
  %24 = getelementptr inbounds i8, i8* %21, i64 48
  %25 = bitcast i8* %24 to %"class.NES::Runtime::BufferRecycler"**
  store %"class.NES::Runtime::BufferRecycler"* %3, %"class.NES::Runtime::BufferRecycler"** %25, align 8, !tbaa !52
  %26 = getelementptr inbounds i8, i8* %21, i64 56
  %27 = getelementptr inbounds i8, i8* %21, i64 72
  %28 = bitcast i8* %27 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %28, align 8, !tbaa !53
  %29 = bitcast { i64, i64 }* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %29)
  %30 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %29, i8* noundef nonnull align 8 dereferenceable(16) %30, i64 16, i1 false) #18, !tbaa.struct !55
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %30, i8* noundef nonnull align 8 dereferenceable(16) %26, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %26, i8* noundef nonnull align 8 dereferenceable(16) %29, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %29)
  %31 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  %32 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %31, align 8, !tbaa !57
  %33 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %28, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %33, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %31, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %32, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %28, align 8, !tbaa !57
  %34 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %35 = getelementptr inbounds i8, i8* %21, i64 80
  %36 = bitcast i8* %35 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  %37 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %34, align 8, !tbaa !57
  %38 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %36, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %38, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %34, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %37, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %36, align 8, !tbaa !57
  %39 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"** %19 to i8**
  store i8* %21, i8** %39, align 8, !tbaa !50
  %40 = load i8*, i8** %17, align 8, !tbaa !47
  %41 = icmp eq i8* %40, null
  br i1 %41, label %42, label %142

42:                                               ; preds = %5
  %43 = bitcast %"class.std::__cxx11::basic_string"* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %43) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %7)
  %44 = bitcast %"class.std::__cxx11::basic_stringbuf"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %44) #18
  %45 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !58
  %46 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 1
  %47 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 7
  %48 = bitcast i8** %46 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %48, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %47) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !58
  %49 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 1
  store i32 24, i32* %49, align 8, !tbaa !60
  %50 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2
  %51 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 2
  %52 = bitcast %"class.std::__cxx11::basic_string"* %50 to %union.anon**
  store %union.anon* %51, %union.anon** %52, align 8, !tbaa !65
  %53 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 1
  store i64 0, i64* %53, align 8, !tbaa !66
  %54 = bitcast %union.anon* %51 to i8*
  store i8 0, i8* %54, align 8, !tbaa !56
  %55 = bitcast %"class.std::basic_ostream"* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %55) #18
  %56 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0
  %57 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1
  %58 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %57, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %58) #18
  %59 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %57, i64 0, i32 0, i32 0
  %60 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %60, align 8, !tbaa !67
  %61 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 2
  store i8 0, i8* %61, align 8, !tbaa !70
  %62 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 3
  store i8 0, i8* %62, align 1, !tbaa !71
  %63 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 4
  %64 = bitcast %"class.std::basic_streambuf"** %63 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %64, i8 0, i64 32, i1 false) #18
  %65 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %65, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %59, align 8, !tbaa !58
  %66 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %67 = getelementptr inbounds i8, i8* %55, i64 %66
  %68 = bitcast i8* %67 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %68, %"class.std::basic_streambuf"* nonnull %56)
          to label %71 unwind label %69

69:                                               ; preds = %42
  %70 = landingpad { i8*, i32 }
          cleanup
  br label %127

71:                                               ; preds = %42
  %72 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([32 x i8], [32 x i8]* @.str, i64 0, i64 0), i64 31)
          to label %73 unwind label %112

73:                                               ; preds = %71
  %74 = call i8* @__cxa_allocate_exception(i64 40) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !72)
  %75 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 2
  %76 = bitcast %"class.std::__cxx11::basic_string"* %10 to %union.anon**
  store %union.anon* %75, %union.anon** %76, align 8, !tbaa !65, !alias.scope !72
  %77 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 1
  store i64 0, i64* %77, align 8, !tbaa !66, !alias.scope !72
  %78 = bitcast %union.anon* %75 to i8*
  store i8 0, i8* %78, align 8, !tbaa !56, !alias.scope !72
  %79 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 5
  %80 = load i8*, i8** %79, align 8, !tbaa !75, !noalias !72
  %81 = icmp eq i8* %80, null
  br i1 %81, label %104, label %82

82:                                               ; preds = %73
  %83 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 3
  %84 = load i8*, i8** %83, align 8, !tbaa !78, !noalias !72
  %85 = icmp ugt i8* %80, %84
  %86 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 4
  %87 = load i8*, i8** %86, align 8, !tbaa !79
  br i1 %85, label %88, label %99

88:                                               ; preds = %82
  %89 = ptrtoint i8* %80 to i64
  %90 = ptrtoint i8* %87 to i64
  %91 = sub i64 %89, %90
  %92 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, i64 0, i64 0, i8* %87, i64 %91)
          to label %105 unwind label %93

93:                                               ; preds = %104, %99, %88
  %94 = landingpad { i8*, i32 }
          cleanup
  %95 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %96 = load i8*, i8** %95, align 8, !tbaa !80, !alias.scope !72
  %97 = icmp eq i8* %96, %78
  br i1 %97, label %122, label %98

98:                                               ; preds = %93
  call void @_ZdlPv(i8* %96) #18
  br label %122

99:                                               ; preds = %82
  %100 = ptrtoint i8* %84 to i64
  %101 = ptrtoint i8* %87 to i64
  %102 = sub i64 %100, %101
  %103 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, i64 0, i64 0, i8* %87, i64 %102)
          to label %105 unwind label %93

104:                                              ; preds = %73
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %50)
          to label %105 unwind label %93

105:                                              ; preds = %104, %99, %88
  %106 = bitcast i8* %74 to %"class.NES::Exceptions::RuntimeException"*
  %107 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %11, i64 0, i32 0
  %108 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %11, i64 0, i32 1
  %109 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %11, i64 0, i32 2
  %110 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %11, i64 0, i32 3
  store i8* getelementptr inbounds ([20 x i8], [20 x i8]* @.str.1, i64 0, i64 0), i8** %107, align 8, !tbaa !81, !alias.scope !83
  store i8* getelementptr inbounds ([14 x i8], [14 x i8]* @.str.2, i64 0, i64 0), i8** %108, align 8, !tbaa !86, !alias.scope !83
  store i32 45, i32* %109, align 8, !tbaa !87, !alias.scope !83
  store i32 0, i32* %110, align 4, !tbaa !88, !alias.scope !83
  invoke void @_ZN3NES10Exceptions16RuntimeExceptionC1ENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS7_NSt12experimental15fundamentals_v215source_locationE(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %106, %"class.std::__cxx11::basic_string"* nonnull %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %7, %"struct.std::experimental::fundamentals_v2::source_location"* nonnull byval(%"struct.std::experimental::fundamentals_v2::source_location") align 8 %11)
          to label %111 unwind label %114

111:                                              ; preds = %105
  invoke void @__cxa_throw(i8* %74, i8* bitcast (i8** @_ZTIN3NES10Exceptions16RuntimeExceptionE to i8*), i8* bitcast (void (%"class.NES::Exceptions::RuntimeException"*)* @_ZN3NES10Exceptions16RuntimeExceptionD1Ev to i8*)) #19
          to label %248 unwind label %114

112:                                              ; preds = %71
  %113 = landingpad { i8*, i32 }
          cleanup
  br label %124

114:                                              ; preds = %111, %105
  %115 = phi i1 [ false, %111 ], [ true, %105 ]
  %116 = landingpad { i8*, i32 }
          cleanup
  %117 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %118 = load i8*, i8** %117, align 8, !tbaa !80
  %119 = icmp eq i8* %118, %78
  br i1 %119, label %121, label %120

120:                                              ; preds = %114
  call void @_ZdlPv(i8* %118) #18
  br i1 %115, label %122, label %124

121:                                              ; preds = %114
  br i1 %115, label %122, label %124

122:                                              ; preds = %121, %120, %98, %93
  %123 = phi { i8*, i32 } [ %116, %121 ], [ %116, %120 ], [ %94, %93 ], [ %94, %98 ]
  call void @__cxa_free_exception(i8* %74) #18
  br label %124

124:                                              ; preds = %122, %121, %120, %112
  %125 = phi { i8*, i32 } [ %123, %122 ], [ %116, %121 ], [ %113, %112 ], [ %116, %120 ]
  %126 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 0
  br label %127

127:                                              ; preds = %124, %69
  %128 = phi %"class.std::ios_base"* [ %58, %69 ], [ %126, %124 ]
  %129 = phi { i8*, i32 } [ %70, %69 ], [ %125, %124 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %128) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %55) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !58
  %130 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 0, i32 0
  %131 = load i8*, i8** %130, align 8, !tbaa !80
  %132 = icmp eq i8* %131, %54
  br i1 %132, label %134, label %133

133:                                              ; preds = %127
  call void @_ZdlPv(i8* %131) #18
  br label %134

134:                                              ; preds = %133, %127
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %47) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %44) #18
  %135 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %136 = load i8*, i8** %135, align 8, !tbaa !80
  %137 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %138 = bitcast %union.anon* %137 to i8*
  %139 = icmp eq i8* %136, %138
  br i1 %139, label %141, label %140

140:                                              ; preds = %134
  call void @_ZdlPv(i8* %136) #18
  br label %141

141:                                              ; preds = %140, %134
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %43) #18
  br label %246

142:                                              ; preds = %5
  %143 = load i32, i32* %18, align 8, !tbaa !49
  %144 = icmp eq i32 %143, 0
  br i1 %144, label %145, label %245

145:                                              ; preds = %142
  %146 = bitcast %"class.std::__cxx11::basic_string"* %12 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %146) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %12)
  %147 = bitcast %"class.std::__cxx11::basic_stringbuf"* %13 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %147) #18
  %148 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !58
  %149 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 1
  %150 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 7
  %151 = bitcast i8** %149 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %151, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %150) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !58
  %152 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 1
  store i32 24, i32* %152, align 8, !tbaa !60
  %153 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2
  %154 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 2
  %155 = bitcast %"class.std::__cxx11::basic_string"* %153 to %union.anon**
  store %union.anon* %154, %union.anon** %155, align 8, !tbaa !65
  %156 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 1
  store i64 0, i64* %156, align 8, !tbaa !66
  %157 = bitcast %union.anon* %154 to i8*
  store i8 0, i8* %157, align 8, !tbaa !56
  %158 = bitcast %"class.std::basic_ostream"* %14 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %158) #18
  %159 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0
  %160 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1
  %161 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %160, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %161) #18
  %162 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %160, i64 0, i32 0, i32 0
  %163 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %163, align 8, !tbaa !67
  %164 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 2
  store i8 0, i8* %164, align 8, !tbaa !70
  %165 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 3
  store i8 0, i8* %165, align 1, !tbaa !71
  %166 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 4
  %167 = bitcast %"class.std::basic_streambuf"** %166 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %167, i8 0, i64 32, i1 false) #18
  %168 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %168, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %162, align 8, !tbaa !58
  %169 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %170 = getelementptr inbounds i8, i8* %158, i64 %169
  %171 = bitcast i8* %170 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %171, %"class.std::basic_streambuf"* nonnull %159)
          to label %174 unwind label %172

172:                                              ; preds = %145
  %173 = landingpad { i8*, i32 }
          cleanup
  br label %230

174:                                              ; preds = %145
  %175 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([29 x i8], [29 x i8]* @.str.3, i64 0, i64 0), i64 28)
          to label %176 unwind label %215

176:                                              ; preds = %174
  %177 = call i8* @__cxa_allocate_exception(i64 40) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !89)
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 2
  %179 = bitcast %"class.std::__cxx11::basic_string"* %15 to %union.anon**
  store %union.anon* %178, %union.anon** %179, align 8, !tbaa !65, !alias.scope !89
  %180 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 1
  store i64 0, i64* %180, align 8, !tbaa !66, !alias.scope !89
  %181 = bitcast %union.anon* %178 to i8*
  store i8 0, i8* %181, align 8, !tbaa !56, !alias.scope !89
  %182 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 5
  %183 = load i8*, i8** %182, align 8, !tbaa !75, !noalias !89
  %184 = icmp eq i8* %183, null
  br i1 %184, label %207, label %185

185:                                              ; preds = %176
  %186 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 3
  %187 = load i8*, i8** %186, align 8, !tbaa !78, !noalias !89
  %188 = icmp ugt i8* %183, %187
  %189 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 4
  %190 = load i8*, i8** %189, align 8, !tbaa !79
  br i1 %188, label %191, label %202

191:                                              ; preds = %185
  %192 = ptrtoint i8* %183 to i64
  %193 = ptrtoint i8* %190 to i64
  %194 = sub i64 %192, %193
  %195 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, i64 0, i64 0, i8* %190, i64 %194)
          to label %208 unwind label %196

196:                                              ; preds = %207, %202, %191
  %197 = landingpad { i8*, i32 }
          cleanup
  %198 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %199 = load i8*, i8** %198, align 8, !tbaa !80, !alias.scope !89
  %200 = icmp eq i8* %199, %181
  br i1 %200, label %225, label %201

201:                                              ; preds = %196
  call void @_ZdlPv(i8* %199) #18
  br label %225

202:                                              ; preds = %185
  %203 = ptrtoint i8* %187 to i64
  %204 = ptrtoint i8* %190 to i64
  %205 = sub i64 %203, %204
  %206 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, i64 0, i64 0, i8* %190, i64 %205)
          to label %208 unwind label %196

207:                                              ; preds = %176
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %153)
          to label %208 unwind label %196

208:                                              ; preds = %207, %202, %191
  %209 = bitcast i8* %177 to %"class.NES::Exceptions::RuntimeException"*
  %210 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %16, i64 0, i32 0
  %211 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %16, i64 0, i32 1
  %212 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %16, i64 0, i32 2
  %213 = getelementptr inbounds %"struct.std::experimental::fundamentals_v2::source_location", %"struct.std::experimental::fundamentals_v2::source_location"* %16, i64 0, i32 3
  store i8* getelementptr inbounds ([20 x i8], [20 x i8]* @.str.1, i64 0, i64 0), i8** %210, align 8, !tbaa !81, !alias.scope !92
  store i8* getelementptr inbounds ([14 x i8], [14 x i8]* @.str.2, i64 0, i64 0), i8** %211, align 8, !tbaa !86, !alias.scope !92
  store i32 48, i32* %212, align 8, !tbaa !87, !alias.scope !92
  store i32 0, i32* %213, align 4, !tbaa !88, !alias.scope !92
  invoke void @_ZN3NES10Exceptions16RuntimeExceptionC1ENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS7_NSt12experimental15fundamentals_v215source_locationE(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %209, %"class.std::__cxx11::basic_string"* nonnull %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %12, %"struct.std::experimental::fundamentals_v2::source_location"* nonnull byval(%"struct.std::experimental::fundamentals_v2::source_location") align 8 %16)
          to label %214 unwind label %217

214:                                              ; preds = %208
  invoke void @__cxa_throw(i8* %177, i8* bitcast (i8** @_ZTIN3NES10Exceptions16RuntimeExceptionE to i8*), i8* bitcast (void (%"class.NES::Exceptions::RuntimeException"*)* @_ZN3NES10Exceptions16RuntimeExceptionD1Ev to i8*)) #19
          to label %248 unwind label %217

215:                                              ; preds = %174
  %216 = landingpad { i8*, i32 }
          cleanup
  br label %227

217:                                              ; preds = %214, %208
  %218 = phi i1 [ false, %214 ], [ true, %208 ]
  %219 = landingpad { i8*, i32 }
          cleanup
  %220 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %221 = load i8*, i8** %220, align 8, !tbaa !80
  %222 = icmp eq i8* %221, %181
  br i1 %222, label %224, label %223

223:                                              ; preds = %217
  call void @_ZdlPv(i8* %221) #18
  br i1 %218, label %225, label %227

224:                                              ; preds = %217
  br i1 %218, label %225, label %227

225:                                              ; preds = %224, %223, %201, %196
  %226 = phi { i8*, i32 } [ %219, %224 ], [ %219, %223 ], [ %197, %196 ], [ %197, %201 ]
  call void @__cxa_free_exception(i8* %177) #18
  br label %227

227:                                              ; preds = %225, %224, %223, %215
  %228 = phi { i8*, i32 } [ %226, %225 ], [ %219, %224 ], [ %216, %215 ], [ %219, %223 ]
  %229 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 0
  br label %230

230:                                              ; preds = %227, %172
  %231 = phi %"class.std::ios_base"* [ %161, %172 ], [ %229, %227 ]
  %232 = phi { i8*, i32 } [ %173, %172 ], [ %228, %227 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %231) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %158) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !58
  %233 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 0, i32 0
  %234 = load i8*, i8** %233, align 8, !tbaa !80
  %235 = icmp eq i8* %234, %157
  br i1 %235, label %237, label %236

236:                                              ; preds = %230
  call void @_ZdlPv(i8* %234) #18
  br label %237

237:                                              ; preds = %236, %230
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %150) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %147) #18
  %238 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 0, i32 0
  %239 = load i8*, i8** %238, align 8, !tbaa !80
  %240 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 2
  %241 = bitcast %union.anon* %240 to i8*
  %242 = icmp eq i8* %239, %241
  br i1 %242, label %244, label %243

243:                                              ; preds = %237
  call void @_ZdlPv(i8* %239) #18
  br label %244

244:                                              ; preds = %243, %237
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %146) #18
  br label %246

245:                                              ; preds = %142
  ret void

246:                                              ; preds = %244, %141
  %247 = phi { i8*, i32 } [ %232, %244 ], [ %129, %141 ]
  resume { i8*, i32 } %247

248:                                              ; preds = %214, %111
  unreachable
}

declare dso_local i32 @__gxx_personality_v0(...)

; Function Attrs: argmemonly mustprogress nofree nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly %0, i8 %1, i64 %2, i1 immarg %3) #5

; Function Attrs: argmemonly mustprogress nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg %0, i8* nocapture %1) #6

; Function Attrs: argmemonly mustprogress nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg %0, i8* nocapture %1) #6

declare dso_local void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* sret(%"class.std::__cxx11::basic_string") align 8 %0) local_unnamed_addr #7

; Function Attrs: nounwind
declare dso_local void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %0) unnamed_addr #8

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %0) unnamed_addr #8

declare dso_local void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %0, %"class.std::basic_streambuf"* %1) local_unnamed_addr #7

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %0, i8* %1, i64 %2) local_unnamed_addr #7

declare dso_local i8* @__cxa_allocate_exception(i64 %0) local_unnamed_addr

; Function Attrs: inaccessiblememonly mustprogress nofree nosync nounwind willreturn
declare void @llvm.experimental.noalias.scope.decl(metadata %0) #9

declare dso_local nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %0, i64 %1, i64 %2, i8* %3, i64 %4) local_unnamed_addr #7

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8* %0) local_unnamed_addr #10

declare dso_local void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %0, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %1) local_unnamed_addr #7

declare dso_local void @_ZN3NES10Exceptions16RuntimeExceptionC1ENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS7_NSt12experimental15fundamentals_v215source_locationE(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %0, %"class.std::__cxx11::basic_string"* %1, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %2, %"struct.std::experimental::fundamentals_v2::source_location"* byval(%"struct.std::experimental::fundamentals_v2::source_location") align 8 %3) unnamed_addr #7

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN3NES10Exceptions16RuntimeExceptionD1Ev(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %0) unnamed_addr #11 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
  %2 = load i32 (...)**, i32 (...)*** bitcast ([2 x i8*]* @_ZTTN3NES10Exceptions16RuntimeExceptionE to i32 (...)***), align 8
  %3 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 0, i32 0
  store i32 (...)** %2, i32 (...)*** %3, align 8, !tbaa !58
  %4 = load i32 (...)**, i32 (...)*** bitcast (i8** getelementptr inbounds ([2 x i8*], [2 x i8*]* @_ZTTN3NES10Exceptions16RuntimeExceptionE, i64 0, i64 1) to i32 (...)***), align 8
  %5 = getelementptr i32 (...)*, i32 (...)** %2, i64 -5
  %6 = bitcast i32 (...)** %5 to i64*
  %7 = load i64, i64* %6, align 8
  %8 = bitcast %"class.NES::Exceptions::RuntimeException"* %0 to i8*
  %9 = getelementptr inbounds i8, i8* %8, i64 %7
  %10 = bitcast i8* %9 to i32 (...)***
  store i32 (...)** %4, i32 (...)*** %10, align 8, !tbaa !58
  %11 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 1, i32 0, i32 0
  %12 = load i8*, i8** %11, align 8, !tbaa !80
  %13 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 1, i32 2
  %14 = bitcast %union.anon* %13 to i8*
  %15 = icmp eq i8* %12, %14
  br i1 %15, label %17, label %16

16:                                               ; preds = %1
  tail call void @_ZdlPv(i8* %12) #18
  br label %17

17:                                               ; preds = %16, %1
  %18 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 0
  tail call void @_ZNSt9exceptionD2Ev(%"class.std::exception"* nonnull dereferenceable(8) %18) #18
  ret void
}

declare dso_local void @__cxa_throw(i8* %0, i8* %1, i8* %2) local_unnamed_addr

declare dso_local void @__cxa_free_exception(i8* %0) local_unnamed_addr

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %0) unnamed_addr #8

; Function Attrs: nounwind
declare dso_local void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %0) unnamed_addr #8

; Function Attrs: nounwind
declare dso_local void @_ZNSt9exceptionD2Ev(%"class.std::exception"* nonnull dereferenceable(8) %0) unnamed_addr #8

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentC2EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EEb(%"class.NES::Runtime::detail::MemorySegment"* nonnull dereferenceable(24) %0, i8* %1, i32 %2, %"class.NES::Runtime::BufferRecycler"* %3, %"class.std::function"* nocapture nonnull align 8 dereferenceable(32) %4, i1 zeroext %5) unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %7 = alloca { i64, i64 }, align 8
  %8 = alloca %"class.std::__cxx11::basic_string", align 8
  %9 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %10 = alloca %"class.std::basic_ostream", align 8
  %11 = alloca %"class.std::__cxx11::basic_string", align 8
  %12 = alloca %"class.std::__cxx11::basic_string", align 8
  %13 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %14 = alloca %"class.std::basic_ostream", align 8
  %15 = alloca %"class.std::__cxx11::basic_string", align 8
  %16 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 0
  store i8* %1, i8** %16, align 8, !tbaa !47
  %17 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 1
  store i32 %2, i32* %17, align 8, !tbaa !49
  %18 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 2
  store %"class.NES::Runtime::detail::BufferControlBlock"* null, %"class.NES::Runtime::detail::BufferControlBlock"** %18, align 8, !tbaa !50
  %19 = icmp eq i8* %1, null
  br i1 %19, label %20, label %134

20:                                               ; preds = %6
  %21 = bitcast %"class.std::__cxx11::basic_string"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %21) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %8)
  %22 = bitcast %"class.std::__cxx11::basic_stringbuf"* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %22) #18
  %23 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !58
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 1
  %25 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 7
  %26 = bitcast i8** %24 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %26, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %25) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !58
  %27 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 1
  store i32 24, i32* %27, align 8, !tbaa !60
  %28 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2
  %29 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 2
  %30 = bitcast %"class.std::__cxx11::basic_string"* %28 to %union.anon**
  store %union.anon* %29, %union.anon** %30, align 8, !tbaa !65
  %31 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 1
  store i64 0, i64* %31, align 8, !tbaa !66
  %32 = bitcast %union.anon* %29 to i8*
  store i8 0, i8* %32, align 8, !tbaa !56
  %33 = bitcast %"class.std::basic_ostream"* %10 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %33) #18
  %34 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0
  %35 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1
  %36 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %35, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %36) #18
  %37 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %35, i64 0, i32 0, i32 0
  %38 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %38, align 8, !tbaa !67
  %39 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 2
  store i8 0, i8* %39, align 8, !tbaa !70
  %40 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 3
  store i8 0, i8* %40, align 1, !tbaa !71
  %41 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 4
  %42 = bitcast %"class.std::basic_streambuf"** %41 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %42, i8 0, i64 32, i1 false) #18
  %43 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %43, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %37, align 8, !tbaa !58
  %44 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %45 = getelementptr inbounds i8, i8* %33, i64 %44
  %46 = bitcast i8* %45 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %46, %"class.std::basic_streambuf"* nonnull %34)
          to label %49 unwind label %47

47:                                               ; preds = %20
  %48 = landingpad { i8*, i32 }
          cleanup
  br label %119

49:                                               ; preds = %20
  %50 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %10, i8* nonnull getelementptr inbounds ([30 x i8], [30 x i8]* @.str.4, i64 0, i64 0), i64 29)
          to label %51 unwind label %107

51:                                               ; preds = %49
  %52 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %10, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %53 unwind label %107

53:                                               ; preds = %51
  %54 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %10, i8* nonnull getelementptr inbounds ([12 x i8], [12 x i8]* @.str.6, i64 0, i64 0), i64 11)
          to label %55 unwind label %107

55:                                               ; preds = %53
  %56 = bitcast %"class.std::__cxx11::basic_string"* %11 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %56) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !95)
  %57 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 2
  %58 = bitcast %"class.std::__cxx11::basic_string"* %11 to %union.anon**
  store %union.anon* %57, %union.anon** %58, align 8, !tbaa !65, !alias.scope !95
  %59 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 1
  store i64 0, i64* %59, align 8, !tbaa !66, !alias.scope !95
  %60 = bitcast %union.anon* %57 to i8*
  store i8 0, i8* %60, align 8, !tbaa !56, !alias.scope !95
  %61 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 5
  %62 = load i8*, i8** %61, align 8, !tbaa !75, !noalias !95
  %63 = icmp eq i8* %62, null
  br i1 %63, label %85, label %64

64:                                               ; preds = %55
  %65 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 3
  %66 = load i8*, i8** %65, align 8, !tbaa !78, !noalias !95
  %67 = icmp ugt i8* %62, %66
  %68 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 4
  %69 = load i8*, i8** %68, align 8, !tbaa !79
  br i1 %67, label %70, label %80

70:                                               ; preds = %64
  %71 = ptrtoint i8* %62 to i64
  %72 = ptrtoint i8* %69 to i64
  %73 = sub i64 %71, %72
  %74 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %11, i64 0, i64 0, i8* %69, i64 %73)
          to label %86 unwind label %75

75:                                               ; preds = %85, %80, %70
  %76 = landingpad { i8*, i32 }
          cleanup
  %77 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 0, i32 0
  %78 = load i8*, i8** %77, align 8, !tbaa !80, !alias.scope !95
  %79 = icmp eq i8* %78, %60
  br i1 %79, label %114, label %.sink.split

80:                                               ; preds = %64
  %81 = ptrtoint i8* %66 to i64
  %82 = ptrtoint i8* %69 to i64
  %83 = sub i64 %81, %82
  %84 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %11, i64 0, i64 0, i8* %69, i64 %83)
          to label %86 unwind label %75

85:                                               ; preds = %55
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %11, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %28)
          to label %86 unwind label %75

86:                                               ; preds = %85, %80, %70
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %11, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %8)
          to label %87 unwind label %109

87:                                               ; preds = %86
  %88 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 0, i32 0
  %89 = load i8*, i8** %88, align 8, !tbaa !80
  %90 = icmp eq i8* %89, %60
  br i1 %90, label %92, label %91

91:                                               ; preds = %87
  call void @_ZdlPv(i8* %89) #18
  br label %92

92:                                               ; preds = %91, %87
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #18
  %93 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %93) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %33) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !58
  %94 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 0, i32 0
  %95 = load i8*, i8** %94, align 8, !tbaa !80
  %96 = icmp eq i8* %95, %32
  br i1 %96, label %98, label %97

97:                                               ; preds = %92
  call void @_ZdlPv(i8* %95) #18
  br label %98

98:                                               ; preds = %97, %92
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %25) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %22) #18
  %99 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 0, i32 0
  %100 = load i8*, i8** %99, align 8, !tbaa !80
  %101 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 2
  %102 = bitcast %union.anon* %101 to i8*
  %103 = icmp eq i8* %100, %102
  br i1 %103, label %105, label %104

104:                                              ; preds = %98
  call void @_ZdlPv(i8* %100) #18
  br label %105

105:                                              ; preds = %104, %98
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %21) #18
  %106 = load i32, i32* %17, align 8, !tbaa !49
  br label %134

107:                                              ; preds = %53, %51, %49
  %108 = landingpad { i8*, i32 }
          cleanup
  br label %116

109:                                              ; preds = %86
  %110 = landingpad { i8*, i32 }
          cleanup
  %111 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 0, i32 0
  %112 = load i8*, i8** %111, align 8, !tbaa !80
  %113 = icmp eq i8* %112, %60
  br i1 %113, label %114, label %.sink.split

.sink.split:                                      ; preds = %109, %75
  %.sink = phi i8* [ %78, %75 ], [ %112, %109 ]
  %.ph = phi { i8*, i32 } [ %76, %75 ], [ %110, %109 ]
  call void @_ZdlPv(i8* %.sink) #18
  br label %114

114:                                              ; preds = %.sink.split, %109, %75
  %115 = phi { i8*, i32 } [ %76, %75 ], [ %110, %109 ], [ %.ph, %.sink.split ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #18
  br label %116

116:                                              ; preds = %114, %107
  %117 = phi { i8*, i32 } [ %115, %114 ], [ %108, %107 ]
  %118 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 0
  br label %119

119:                                              ; preds = %116, %47
  %120 = phi %"class.std::ios_base"* [ %36, %47 ], [ %118, %116 ]
  %121 = phi { i8*, i32 } [ %48, %47 ], [ %117, %116 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %120) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %33) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !58
  %122 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 0, i32 0
  %123 = load i8*, i8** %122, align 8, !tbaa !80
  %124 = icmp eq i8* %123, %32
  br i1 %124, label %126, label %125

125:                                              ; preds = %119
  call void @_ZdlPv(i8* %123) #18
  br label %126

126:                                              ; preds = %125, %119
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %25) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %22) #18
  %127 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 0, i32 0
  %128 = load i8*, i8** %127, align 8, !tbaa !80
  %129 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 2
  %130 = bitcast %union.anon* %129 to i8*
  %131 = icmp eq i8* %128, %130
  br i1 %131, label %133, label %132

132:                                              ; preds = %126
  call void @_ZdlPv(i8* %128) #18
  br label %133

133:                                              ; preds = %132, %126
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %21) #18
  br label %270

134:                                              ; preds = %105, %6
  %135 = phi i32 [ %106, %105 ], [ %2, %6 ]
  %136 = icmp eq i32 %135, 0
  br i1 %136, label %137, label %250

137:                                              ; preds = %134
  %138 = bitcast %"class.std::__cxx11::basic_string"* %12 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %138) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %12)
  %139 = bitcast %"class.std::__cxx11::basic_stringbuf"* %13 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %139) #18
  %140 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %141 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 1
  %142 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 7
  %143 = bitcast i8** %141 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %143, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %142) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %144 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 1
  store i32 24, i32* %144, align 8, !tbaa !60
  %145 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2
  %146 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 2
  %147 = bitcast %"class.std::__cxx11::basic_string"* %145 to %union.anon**
  store %union.anon* %146, %union.anon** %147, align 8, !tbaa !65
  %148 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 1
  store i64 0, i64* %148, align 8, !tbaa !66
  %149 = bitcast %union.anon* %146 to i8*
  store i8 0, i8* %149, align 8, !tbaa !56
  %150 = bitcast %"class.std::basic_ostream"* %14 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %150) #18
  %151 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0
  %152 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1
  %153 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %152, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %153) #18
  %154 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %152, i64 0, i32 0, i32 0
  %155 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %155, align 8, !tbaa !67
  %156 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 2
  store i8 0, i8* %156, align 8, !tbaa !70
  %157 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 3
  store i8 0, i8* %157, align 1, !tbaa !71
  %158 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 4
  %159 = bitcast %"class.std::basic_streambuf"** %158 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %159, i8 0, i64 32, i1 false) #18
  %160 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %160, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %154, align 8, !tbaa !58
  %161 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %162 = getelementptr inbounds i8, i8* %150, i64 %161
  %163 = bitcast i8* %162 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %163, %"class.std::basic_streambuf"* nonnull %151)
          to label %166 unwind label %164

164:                                              ; preds = %137
  %165 = landingpad { i8*, i32 }
          cleanup
  br label %235

166:                                              ; preds = %137
  %167 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([31 x i8], [31 x i8]* @.str.7, i64 0, i64 0), i64 30)
          to label %168 unwind label %223

168:                                              ; preds = %166
  %169 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %170 unwind label %223

170:                                              ; preds = %168
  %171 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([13 x i8], [13 x i8]* @.str.8, i64 0, i64 0), i64 12)
          to label %172 unwind label %223

172:                                              ; preds = %170
  %173 = bitcast %"class.std::__cxx11::basic_string"* %15 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %173) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !98)
  %174 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 2
  %175 = bitcast %"class.std::__cxx11::basic_string"* %15 to %union.anon**
  store %union.anon* %174, %union.anon** %175, align 8, !tbaa !65, !alias.scope !98
  %176 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 1
  store i64 0, i64* %176, align 8, !tbaa !66, !alias.scope !98
  %177 = bitcast %union.anon* %174 to i8*
  store i8 0, i8* %177, align 8, !tbaa !56, !alias.scope !98
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 5
  %179 = load i8*, i8** %178, align 8, !tbaa !75, !noalias !98
  %180 = icmp eq i8* %179, null
  br i1 %180, label %202, label %181

181:                                              ; preds = %172
  %182 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 3
  %183 = load i8*, i8** %182, align 8, !tbaa !78, !noalias !98
  %184 = icmp ugt i8* %179, %183
  %185 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 4
  %186 = load i8*, i8** %185, align 8, !tbaa !79
  br i1 %184, label %187, label %197

187:                                              ; preds = %181
  %188 = ptrtoint i8* %179 to i64
  %189 = ptrtoint i8* %186 to i64
  %190 = sub i64 %188, %189
  %191 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, i64 0, i64 0, i8* %186, i64 %190)
          to label %203 unwind label %192

192:                                              ; preds = %202, %197, %187
  %193 = landingpad { i8*, i32 }
          cleanup
  %194 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %195 = load i8*, i8** %194, align 8, !tbaa !80, !alias.scope !98
  %196 = icmp eq i8* %195, %177
  br i1 %196, label %230, label %.sink.split21

197:                                              ; preds = %181
  %198 = ptrtoint i8* %183 to i64
  %199 = ptrtoint i8* %186 to i64
  %200 = sub i64 %198, %199
  %201 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, i64 0, i64 0, i8* %186, i64 %200)
          to label %203 unwind label %192

202:                                              ; preds = %172
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %145)
          to label %203 unwind label %192

203:                                              ; preds = %202, %197, %187
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %12)
          to label %204 unwind label %225

204:                                              ; preds = %203
  %205 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %206 = load i8*, i8** %205, align 8, !tbaa !80
  %207 = icmp eq i8* %206, %177
  br i1 %207, label %209, label %208

208:                                              ; preds = %204
  call void @_ZdlPv(i8* %206) #18
  br label %209

209:                                              ; preds = %208, %204
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %173) #18
  %210 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %210) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %150) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %211 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 0, i32 0
  %212 = load i8*, i8** %211, align 8, !tbaa !80
  %213 = icmp eq i8* %212, %149
  br i1 %213, label %215, label %214

214:                                              ; preds = %209
  call void @_ZdlPv(i8* %212) #18
  br label %215

215:                                              ; preds = %214, %209
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %142) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %139) #18
  %216 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 0, i32 0
  %217 = load i8*, i8** %216, align 8, !tbaa !80
  %218 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 2
  %219 = bitcast %union.anon* %218 to i8*
  %220 = icmp eq i8* %217, %219
  br i1 %220, label %222, label %221

221:                                              ; preds = %215
  call void @_ZdlPv(i8* %217) #18
  br label %222

222:                                              ; preds = %221, %215
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %138) #18
  br label %250

223:                                              ; preds = %170, %168, %166
  %224 = landingpad { i8*, i32 }
          cleanup
  br label %232

225:                                              ; preds = %203
  %226 = landingpad { i8*, i32 }
          cleanup
  %227 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %228 = load i8*, i8** %227, align 8, !tbaa !80
  %229 = icmp eq i8* %228, %177
  br i1 %229, label %230, label %.sink.split21

.sink.split21:                                    ; preds = %225, %192
  %.sink23 = phi i8* [ %195, %192 ], [ %228, %225 ]
  %.ph22 = phi { i8*, i32 } [ %193, %192 ], [ %226, %225 ]
  call void @_ZdlPv(i8* %.sink23) #18
  br label %230

230:                                              ; preds = %.sink.split21, %225, %192
  %231 = phi { i8*, i32 } [ %193, %192 ], [ %226, %225 ], [ %.ph22, %.sink.split21 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %173) #18
  br label %232

232:                                              ; preds = %230, %223
  %233 = phi { i8*, i32 } [ %231, %230 ], [ %224, %223 ]
  %234 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 0
  br label %235

235:                                              ; preds = %232, %164
  %236 = phi %"class.std::ios_base"* [ %153, %164 ], [ %234, %232 ]
  %237 = phi { i8*, i32 } [ %165, %164 ], [ %233, %232 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %236) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %150) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %238 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 0, i32 0
  %239 = load i8*, i8** %238, align 8, !tbaa !80
  %240 = icmp eq i8* %239, %149
  br i1 %240, label %242, label %241

241:                                              ; preds = %235
  call void @_ZdlPv(i8* %239) #18
  br label %242

242:                                              ; preds = %241, %235
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %142) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %139) #18
  %243 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 0, i32 0
  %244 = load i8*, i8** %243, align 8, !tbaa !80
  %245 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 2
  %246 = bitcast %union.anon* %245 to i8*
  %247 = icmp eq i8* %244, %246
  br i1 %247, label %249, label %248

248:                                              ; preds = %242
  call void @_ZdlPv(i8* %244) #18
  br label %249

249:                                              ; preds = %248, %242
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %138) #18
  br label %270

250:                                              ; preds = %222, %134
  %251 = call noalias nonnull dereferenceable(88) i8* @_Znwm(i64 88) #20
  %252 = getelementptr inbounds i8, i8* %251, i64 40
  %253 = bitcast i8* %252 to %"class.NES::Runtime::detail::MemorySegment"**
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 4 dereferenceable(40) %251, i8 0, i64 40, i1 false) #18
  store %"class.NES::Runtime::detail::MemorySegment"* %0, %"class.NES::Runtime::detail::MemorySegment"** %253, align 8, !tbaa !51
  %254 = getelementptr inbounds i8, i8* %251, i64 48
  %255 = bitcast i8* %254 to %"class.NES::Runtime::BufferRecycler"**
  store %"class.NES::Runtime::BufferRecycler"* %3, %"class.NES::Runtime::BufferRecycler"** %255, align 8, !tbaa !52
  %256 = getelementptr inbounds i8, i8* %251, i64 56
  %257 = getelementptr inbounds i8, i8* %251, i64 72
  %258 = bitcast i8* %257 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  %259 = bitcast { i64, i64 }* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %259)
  %260 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %259, i8* noundef nonnull align 8 dereferenceable(16) %260, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %260, i8* noundef nonnull align 8 dereferenceable(16) %256, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %256, i8* noundef nonnull align 8 dereferenceable(16) %259, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %259)
  %261 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  %262 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %261, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %261, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %262, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %258, align 8, !tbaa !57
  %263 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %264 = getelementptr inbounds i8, i8* %251, i64 80
  %265 = bitcast i8* %264 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  %266 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %263, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %266, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %265, align 8, !tbaa !57
  %267 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"** %18 to i8**
  store i8* %251, i8** %267, align 8, !tbaa !50
  %268 = bitcast i8* %251 to i32*
  %269 = cmpxchg i32* %268, i32 0, i32 1 seq_cst seq_cst, align 4
  ret void

270:                                              ; preds = %249, %133
  %271 = phi { i8*, i32 } [ %237, %249 ], [ %121, %133 ]
  resume { i8*, i32 } %271
}

declare dso_local void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %0, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %1) local_unnamed_addr #7

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64 %0) local_unnamed_addr #12

; Function Attrs: mustprogress nofree norecurse nounwind uwtable willreturn
define dso_local zeroext i1 @_ZN3NES7Runtime6detail18BufferControlBlock7prepareEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0) local_unnamed_addr #13 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %3 = cmpxchg i32* %2, i32 0, i32 1 seq_cst seq_cst, align 4
  %4 = extractvalue { i32, i1 } %3, 1
  ret i1 %4
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentD2Ev(%"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull dereferenceable(24) %0) unnamed_addr #11 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %2 = alloca %"class.std::__cxx11::basic_string", align 8
  %3 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %4 = alloca %"class.std::basic_ostream", align 8
  %5 = alloca %"class.std::__cxx11::basic_string", align 8
  %6 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 0
  %7 = load i8*, i8** %6, align 8, !tbaa !47
  %8 = icmp eq i8* %7, null
  br i1 %8, label %170, label %9

9:                                                ; preds = %1
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 2
  %11 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %10, align 8, !tbaa !50
  %12 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %11, i64 0, i32 0, i32 0, i32 0
  %13 = load atomic i32, i32* %12 seq_cst, align 4
  %14 = icmp eq i32 %13, 0
  br i1 %14, label %137, label %15

15:                                               ; preds = %9
  %16 = bitcast %"class.std::__cxx11::basic_string"* %2 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %16) #18
  invoke void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %2)
          to label %17 unwind label %106

17:                                               ; preds = %15
  %18 = bitcast %"class.std::__cxx11::basic_stringbuf"* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %18) #18
  %19 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !58
  %20 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 1
  %21 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 7
  %22 = bitcast i8** %20 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %22, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %21) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !58
  %23 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 1
  store i32 24, i32* %23, align 8, !tbaa !60
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2
  %25 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 2
  %26 = bitcast %"class.std::__cxx11::basic_string"* %24 to %union.anon**
  store %union.anon* %25, %union.anon** %26, align 8, !tbaa !65
  %27 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 1
  store i64 0, i64* %27, align 8, !tbaa !66
  %28 = bitcast %union.anon* %25 to i8*
  store i8 0, i8* %28, align 8, !tbaa !56
  %29 = bitcast %"class.std::basic_ostream"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %29) #18
  %30 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0
  %31 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1
  %32 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %31, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %32) #18
  %33 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %31, i64 0, i32 0, i32 0
  %34 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %34, align 8, !tbaa !67
  %35 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 2
  store i8 0, i8* %35, align 8, !tbaa !70
  %36 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 3
  store i8 0, i8* %36, align 1, !tbaa !71
  %37 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 4
  %38 = bitcast %"class.std::basic_streambuf"** %37 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %38, i8 0, i64 32, i1 false) #18
  %39 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %39, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %33, align 8, !tbaa !58
  %40 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %41 = getelementptr inbounds i8, i8* %29, i64 %40
  %42 = bitcast i8* %41 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %42, %"class.std::basic_streambuf"* nonnull %30)
          to label %45 unwind label %43

43:                                               ; preds = %17
  %44 = landingpad { i8*, i32 }
          catch i8* null
  br label %120

45:                                               ; preds = %17
  %46 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %4, i8* nonnull getelementptr inbounds ([32 x i8], [32 x i8]* @.str.9, i64 0, i64 0), i64 31)
          to label %47 unwind label %108

47:                                               ; preds = %45
  %48 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %4, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %49 unwind label %108

49:                                               ; preds = %47
  %50 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %4, i8* nonnull getelementptr inbounds ([42 x i8], [42 x i8]* @.str.10, i64 0, i64 0), i64 41)
          to label %51 unwind label %108

51:                                               ; preds = %49
  %52 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8) %4, i32 %13)
          to label %53 unwind label %108

53:                                               ; preds = %51
  %54 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %52, i8* nonnull getelementptr inbounds ([21 x i8], [21 x i8]* @.str.11, i64 0, i64 0), i64 20)
          to label %55 unwind label %108

55:                                               ; preds = %53
  %56 = bitcast %"class.std::__cxx11::basic_string"* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %56) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !101)
  %57 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 2
  %58 = bitcast %"class.std::__cxx11::basic_string"* %5 to %union.anon**
  store %union.anon* %57, %union.anon** %58, align 8, !tbaa !65, !alias.scope !101
  %59 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 1
  store i64 0, i64* %59, align 8, !tbaa !66, !alias.scope !101
  %60 = bitcast %union.anon* %57 to i8*
  store i8 0, i8* %60, align 8, !tbaa !56, !alias.scope !101
  %61 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 5
  %62 = load i8*, i8** %61, align 8, !tbaa !75, !noalias !101
  %63 = icmp eq i8* %62, null
  br i1 %63, label %85, label %64

64:                                               ; preds = %55
  %65 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 3
  %66 = load i8*, i8** %65, align 8, !tbaa !78, !noalias !101
  %67 = icmp ugt i8* %62, %66
  %68 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 4
  %69 = load i8*, i8** %68, align 8, !tbaa !79
  br i1 %67, label %70, label %80

70:                                               ; preds = %64
  %71 = ptrtoint i8* %62 to i64
  %72 = ptrtoint i8* %69 to i64
  %73 = sub i64 %71, %72
  %74 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %5, i64 0, i64 0, i8* %69, i64 %73)
          to label %86 unwind label %75

75:                                               ; preds = %85, %80, %70
  %76 = landingpad { i8*, i32 }
          catch i8* null
  %77 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 0, i32 0
  %78 = load i8*, i8** %77, align 8, !tbaa !80, !alias.scope !101
  %79 = icmp eq i8* %78, %60
  br i1 %79, label %115, label %.sink.split

80:                                               ; preds = %64
  %81 = ptrtoint i8* %66 to i64
  %82 = ptrtoint i8* %69 to i64
  %83 = sub i64 %81, %82
  %84 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %5, i64 0, i64 0, i8* %69, i64 %83)
          to label %86 unwind label %75

85:                                               ; preds = %55
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %5, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %24)
          to label %86 unwind label %75

86:                                               ; preds = %85, %80, %70
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %5, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %2)
          to label %87 unwind label %110

87:                                               ; preds = %86
  %88 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 0, i32 0
  %89 = load i8*, i8** %88, align 8, !tbaa !80
  %90 = icmp eq i8* %89, %60
  br i1 %90, label %92, label %91

91:                                               ; preds = %87
  call void @_ZdlPv(i8* %89) #18
  br label %92

92:                                               ; preds = %91, %87
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #18
  %93 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %93) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %29) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !58
  %94 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 0, i32 0
  %95 = load i8*, i8** %94, align 8, !tbaa !80
  %96 = icmp eq i8* %95, %28
  br i1 %96, label %98, label %97

97:                                               ; preds = %92
  call void @_ZdlPv(i8* %95) #18
  br label %98

98:                                               ; preds = %97, %92
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %21) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %18) #18
  %99 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 0, i32 0
  %100 = load i8*, i8** %99, align 8, !tbaa !80
  %101 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 2
  %102 = bitcast %union.anon* %101 to i8*
  %103 = icmp eq i8* %100, %102
  br i1 %103, label %105, label %104

104:                                              ; preds = %98
  call void @_ZdlPv(i8* %100) #18
  br label %105

105:                                              ; preds = %104, %98
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %16) #18
  br label %137

106:                                              ; preds = %15
  %107 = landingpad { i8*, i32 }
          catch i8* null
  br label %134

108:                                              ; preds = %53, %51, %49, %47, %45
  %109 = landingpad { i8*, i32 }
          catch i8* null
  br label %117

110:                                              ; preds = %86
  %111 = landingpad { i8*, i32 }
          catch i8* null
  %112 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 0, i32 0
  %113 = load i8*, i8** %112, align 8, !tbaa !80
  %114 = icmp eq i8* %113, %60
  br i1 %114, label %115, label %.sink.split

.sink.split:                                      ; preds = %110, %75
  %.sink = phi i8* [ %78, %75 ], [ %113, %110 ]
  %.ph = phi { i8*, i32 } [ %76, %75 ], [ %111, %110 ]
  call void @_ZdlPv(i8* %.sink) #18
  br label %115

115:                                              ; preds = %.sink.split, %110, %75
  %116 = phi { i8*, i32 } [ %76, %75 ], [ %111, %110 ], [ %.ph, %.sink.split ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #18
  br label %117

117:                                              ; preds = %115, %108
  %118 = phi { i8*, i32 } [ %116, %115 ], [ %109, %108 ]
  %119 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 0
  br label %120

120:                                              ; preds = %117, %43
  %121 = phi %"class.std::ios_base"* [ %32, %43 ], [ %119, %117 ]
  %122 = phi { i8*, i32 } [ %44, %43 ], [ %118, %117 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %121) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %29) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !58
  %123 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 0, i32 0
  %124 = load i8*, i8** %123, align 8, !tbaa !80
  %125 = icmp eq i8* %124, %28
  br i1 %125, label %127, label %126

126:                                              ; preds = %120
  call void @_ZdlPv(i8* %124) #18
  br label %127

127:                                              ; preds = %126, %120
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %21) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %18) #18
  %128 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 0, i32 0
  %129 = load i8*, i8** %128, align 8, !tbaa !80
  %130 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 2
  %131 = bitcast %union.anon* %130 to i8*
  %132 = icmp eq i8* %129, %131
  br i1 %132, label %134, label %133

133:                                              ; preds = %127
  call void @_ZdlPv(i8* %129) #18
  br label %134

134:                                              ; preds = %133, %127, %106
  %135 = phi { i8*, i32 } [ %107, %106 ], [ %122, %127 ], [ %122, %133 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %16) #18
  %136 = extractvalue { i8*, i32 } %135, 0
  call void @__clang_call_terminate(i8* %136) #21
  unreachable

137:                                              ; preds = %105, %9
  %138 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 1
  %139 = load i32, i32* %138, align 8, !tbaa !49
  %140 = zext i32 %139 to i64
  %141 = load i8*, i8** %6, align 8, !tbaa !47
  %142 = getelementptr inbounds i8, i8* %141, i64 %140
  %143 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %10, align 8, !tbaa !50
  %144 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"* %143 to i8*
  %145 = icmp eq i8* %142, %144
  br i1 %145, label %159, label %146

146:                                              ; preds = %137
  %147 = icmp eq %"class.NES::Runtime::detail::BufferControlBlock"* %143, null
  br i1 %147, label %169, label %148

148:                                              ; preds = %146
  %149 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %143, i64 0, i32 8, i32 0, i32 1
  %150 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %149, align 8, !tbaa !53
  %151 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %150, null
  br i1 %151, label %158, label %152

152:                                              ; preds = %148
  %153 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %143, i64 0, i32 8, i32 0, i32 0
  %154 = invoke zeroext i1 %150(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %153, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %153, i32 3)
          to label %158 unwind label %155

155:                                              ; preds = %152
  %156 = landingpad { i8*, i32 }
          catch i8* null
  %157 = extractvalue { i8*, i32 } %156, 0
  call void @__clang_call_terminate(i8* %157) #21
  unreachable

158:                                              ; preds = %152, %148
  call void @_ZdlPv(i8* nonnull %144) #22
  br label %169

159:                                              ; preds = %137
  %160 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %143, i64 0, i32 8, i32 0, i32 1
  %161 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %160, align 8, !tbaa !53
  %162 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %161, null
  br i1 %162, label %169, label %163

163:                                              ; preds = %159
  %164 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %143, i64 0, i32 8, i32 0, i32 0
  %165 = invoke zeroext i1 %161(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %164, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %164, i32 3)
          to label %169 unwind label %166

166:                                              ; preds = %163
  %167 = landingpad { i8*, i32 }
          catch i8* null
  %168 = extractvalue { i8*, i32 } %167, 0
  call void @__clang_call_terminate(i8* %168) #21
  unreachable

169:                                              ; preds = %163, %159, %158, %146
  store i8* null, i8** %6, align 8, !tbaa !57
  br label %170

170:                                              ; preds = %169, %1
  ret void
}

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8) %0, i32 %1) local_unnamed_addr #7

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #14 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #18
  tail call void @_ZSt9terminatev() #21
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8* %0) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

; Function Attrs: mustprogress nofree norecurse nounwind uwtable willreturn
define dso_local i32 @_ZNK3NES7Runtime6detail18BufferControlBlock17getReferenceCountEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #13 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %3 = load atomic i32, i32* %2 seq_cst, align 4
  ret i32 %3
}

; Function Attrs: mustprogress nofree nosync nounwind uwtable willreturn
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlockC2EPNS1_13MemorySegmentEPNS0_14BufferRecyclerEOSt8functionIFvS4_S6_EE(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, %"class.NES::Runtime::detail::MemorySegment"* %1, %"class.NES::Runtime::BufferRecycler"* %2, %"class.std::function"* nocapture nonnull align 8 dereferenceable(32) %3) unnamed_addr #2 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %5 = alloca { i64, i64 }, align 8
  %6 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  %7 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"* %0 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 4 dereferenceable(40) %7, i8 0, i64 40, i1 false)
  store %"class.NES::Runtime::detail::MemorySegment"* %1, %"class.NES::Runtime::detail::MemorySegment"** %6, align 8, !tbaa !51
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7, i32 0, i32 0
  store %"class.NES::Runtime::BufferRecycler"* %2, %"class.NES::Runtime::BufferRecycler"** %8, align 8, !tbaa !52
  %9 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  %11 = bitcast { i64, i64 }* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %11)
  %12 = bitcast %"class.std::function"* %3 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %11, i8* noundef nonnull align 8 dereferenceable(16) %12, i64 16, i1 false) #18, !tbaa.struct !55
  %13 = bitcast %"class.std::function"* %9 to i8*
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %12, i8* noundef nonnull align 8 dereferenceable(16) %13, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %13, i8* noundef nonnull align 8 dereferenceable(16) %11, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %11)
  %14 = getelementptr inbounds %"class.std::function", %"class.std::function"* %3, i64 0, i32 0, i32 1
  %15 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !57
  %16 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %16, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %15, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !57
  %17 = getelementptr inbounds %"class.std::function", %"class.std::function"* %3, i64 0, i32 1
  %18 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %19 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %17, align 8, !tbaa !57
  %20 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %18, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %20, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %17, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %19, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %18, align 8, !tbaa !57
  ret void
}

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlockC2ERKS2_(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %0, %"class.NES::Runtime::detail::BufferControlBlock"* nonnull align 8 dereferenceable(88) %1) unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %3 = alloca { i64, i64 }, align 8
  %4 = alloca %"class.std::function", align 8
  %5 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %6 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  %7 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7
  %9 = bitcast %"struct.std::atomic.0"* %8 to i64*
  store i64 0, i64* %9, align 8
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %11 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !53
  %12 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 0, i32 0, i32 0
  %13 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"* %0 to i8*
  tail call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 4 dereferenceable(40) %13, i8 0, i64 40, i1 false)
  %14 = load atomic i32, i32* %12 seq_cst, align 8
  store atomic i32 %14, i32* %5 seq_cst, align 4
  %15 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 1
  %16 = load i32, i32* %15, align 4, !tbaa !17
  store i32 %16, i32* %6, align 4, !tbaa !17
  %17 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 4
  %18 = load i64, i64* %17, align 8, !tbaa !104
  store i64 %18, i64* %7, align 8, !tbaa !104
  %19 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %19) #18
  %20 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %20, align 8, !tbaa !53
  %21 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 1
  %22 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %21, align 8, !tbaa !53
  %23 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %22, null
  br i1 %23, label %41, label %24

24:                                               ; preds = %2
  %25 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %26 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 0
  %27 = invoke zeroext i1 %22(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %25, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %26, i32 2)
          to label %28 unwind label %32

28:                                               ; preds = %24
  %29 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 1
  %30 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %29, align 8, !tbaa !105
  %31 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %21, align 8, !tbaa !53
  br label %41

32:                                               ; preds = %24
  %33 = landingpad { i8*, i32 }
          cleanup
  %34 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %20, align 8, !tbaa !53
  %35 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %34, null
  br i1 %35, label %67, label %36

36:                                               ; preds = %32
  %37 = invoke zeroext i1 %34(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %25, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %25, i32 3)
          to label %67 unwind label %38

38:                                               ; preds = %36
  %39 = landingpad { i8*, i32 }
          catch i8* null
  %40 = extractvalue { i8*, i32 } %39, 0
  call void @__clang_call_terminate(i8* %40) #21
  unreachable

41:                                               ; preds = %28, %2
  %42 = phi void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* [ undef, %2 ], [ %30, %28 ]
  %43 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ null, %2 ], [ %31, %28 ]
  %44 = bitcast { i64, i64 }* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %44)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %44, i8* noundef nonnull align 8 dereferenceable(16) %19, i64 16, i1 false) #18, !tbaa.struct !55
  %45 = bitcast %"class.std::function"* %10 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %19, i8* noundef nonnull align 8 dereferenceable(16) %45, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %45, i8* noundef nonnull align 8 dereferenceable(16) %44, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %44)
  %46 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %46, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %20, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %43, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !57
  %47 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %48 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %49 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %48, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %49, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %47, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %42, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %48, align 8, !tbaa !57
  %50 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %46, null
  br i1 %50, label %57, label %51

51:                                               ; preds = %41
  %52 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %53 = invoke zeroext i1 %46(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %52, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %52, i32 3)
          to label %57 unwind label %54

54:                                               ; preds = %51
  %55 = landingpad { i8*, i32 }
          catch i8* null
  %56 = extractvalue { i8*, i32 } %55, 0
  call void @__clang_call_terminate(i8* %56) #21
  unreachable

57:                                               ; preds = %51, %41
  %58 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 5
  %59 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %19) #18
  %60 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 6
  %61 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %60, align 8, !tbaa !51
  %62 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  store %"class.NES::Runtime::detail::MemorySegment"* %61, %"class.NES::Runtime::detail::MemorySegment"** %62, align 8, !tbaa !51
  %63 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 2
  %64 = load i64, i64* %63, align 8, !tbaa !106
  store i64 %64, i64* %59, align 8, !tbaa !106
  %65 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 5
  %66 = load i64, i64* %65, align 8, !tbaa !107
  store i64 %66, i64* %58, align 8, !tbaa !107
  ret void

67:                                               ; preds = %36, %32
  %68 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !53
  %69 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %68, null
  br i1 %69, label %76, label %70

70:                                               ; preds = %67
  %71 = getelementptr inbounds %"class.std::function", %"class.std::function"* %10, i64 0, i32 0, i32 0
  %72 = invoke zeroext i1 %68(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %71, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %71, i32 3)
          to label %76 unwind label %73

73:                                               ; preds = %70
  %74 = landingpad { i8*, i32 }
          catch i8* null
  %75 = extractvalue { i8*, i32 } %74, 0
  call void @__clang_call_terminate(i8* %75) #21
  unreachable

76:                                               ; preds = %70, %67
  resume { i8*, i32 } %33
}

; Function Attrs: uwtable
define dso_local nonnull align 8 dereferenceable(88) %"class.NES::Runtime::detail::BufferControlBlock"* @_ZN3NES7Runtime6detail18BufferControlBlockaSERKS2_(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull returned dereferenceable(88) %0, %"class.NES::Runtime::detail::BufferControlBlock"* nonnull align 8 dereferenceable(88) %1) local_unnamed_addr #4 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %3 = alloca { i64, i64 }, align 8
  %4 = alloca %"class.std::function", align 8
  %5 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 0, i32 0, i32 0
  %6 = load atomic i32, i32* %5 seq_cst, align 8
  %7 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  store atomic i32 %6, i32* %7 seq_cst, align 4
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 1
  %9 = load i32, i32* %8, align 4, !tbaa !17
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  store i32 %9, i32* %10, align 4, !tbaa !17
  %11 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %12 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %12) #18
  %13 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !53
  %14 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 1
  %15 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !53
  %16 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %15, null
  br i1 %16, label %35, label %17

17:                                               ; preds = %2
  %18 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %19 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 0
  %20 = invoke zeroext i1 %15(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %19, i32 2)
          to label %21 unwind label %25

21:                                               ; preds = %17
  %22 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 1
  %23 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %22, align 8, !tbaa !105
  %24 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !53
  br label %35

25:                                               ; preds = %17
  %26 = landingpad { i8*, i32 }
          cleanup
  %27 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !53
  %28 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %27, null
  br i1 %28, label %34, label %29

29:                                               ; preds = %25
  %30 = invoke zeroext i1 %27(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, i32 3)
          to label %34 unwind label %31

31:                                               ; preds = %29
  %32 = landingpad { i8*, i32 }
          catch i8* null
  %33 = extractvalue { i8*, i32 } %32, 0
  call void @__clang_call_terminate(i8* %33) #21
  unreachable

34:                                               ; preds = %29, %25
  resume { i8*, i32 } %26

35:                                               ; preds = %21, %2
  %36 = phi void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* [ undef, %2 ], [ %23, %21 ]
  %37 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ null, %2 ], [ %24, %21 ]
  %38 = bitcast { i64, i64 }* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %38)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %38, i8* noundef nonnull align 8 dereferenceable(16) %12, i64 16, i1 false) #18, !tbaa.struct !55
  %39 = bitcast %"class.std::function"* %11 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %12, i8* noundef nonnull align 8 dereferenceable(16) %39, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %39, i8* noundef nonnull align 8 dereferenceable(16) %38, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %38)
  %40 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  %41 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %41, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %37, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !57
  %42 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %43 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %44 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %43, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %44, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %42, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %36, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %43, align 8, !tbaa !57
  %45 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %41, null
  br i1 %45, label %52, label %46

46:                                               ; preds = %35
  %47 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %48 = invoke zeroext i1 %41(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %47, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %47, i32 3)
          to label %52 unwind label %49

49:                                               ; preds = %46
  %50 = landingpad { i8*, i32 }
          catch i8* null
  %51 = extractvalue { i8*, i32 } %50, 0
  call void @__clang_call_terminate(i8* %51) #21
  unreachable

52:                                               ; preds = %46, %35
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %12) #18
  %53 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 6
  %54 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %53, align 8, !tbaa !51
  %55 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  store %"class.NES::Runtime::detail::MemorySegment"* %54, %"class.NES::Runtime::detail::MemorySegment"** %55, align 8, !tbaa !51
  %56 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 2
  %57 = load i64, i64* %56, align 8, !tbaa !106
  %58 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  store i64 %57, i64* %58, align 8, !tbaa !106
  %59 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 4
  %60 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  %61 = bitcast i64* %59 to <2 x i64>*
  %62 = load <2 x i64>, <2 x i64>* %61, align 8, !tbaa !108
  %63 = bitcast i64* %60 to <2 x i64>*
  store <2 x i64> %62, <2 x i64>* %63, align 8, !tbaa !108
  ret %"class.NES::Runtime::detail::BufferControlBlock"* %0
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local %"class.NES::Runtime::detail::MemorySegment"* @_ZNK3NES7Runtime6detail18BufferControlBlock8getOwnerEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #1 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  %3 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %2, align 8, !tbaa !51
  ret %"class.NES::Runtime::detail::MemorySegment"* %3
}

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock19resetBufferRecyclerEPNS0_14BufferRecyclerE(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, %"class.NES::Runtime::BufferRecycler"* %1) local_unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %3 = alloca %"class.std::__cxx11::basic_string", align 8
  %4 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %5 = alloca %"class.std::basic_ostream", align 8
  %6 = alloca %"class.std::__cxx11::basic_string", align 8
  %7 = alloca %"class.std::__cxx11::basic_string", align 8
  %8 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %9 = alloca %"class.std::basic_ostream", align 8
  %10 = alloca %"class.std::__cxx11::basic_string", align 8
  %11 = icmp eq %"class.NES::Runtime::BufferRecycler"* %1, null
  br i1 %11, label %17, label %12

12:                                               ; preds = %2
  %13 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7
  %14 = bitcast %"struct.std::atomic.0"* %13 to i64*
  %15 = ptrtoint %"class.NES::Runtime::BufferRecycler"* %1 to i64
  %16 = atomicrmw xchg i64* %14, i64 %15 seq_cst, align 8
  br label %133

17:                                               ; preds = %2
  %18 = bitcast %"class.std::__cxx11::basic_string"* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %18) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %3)
  %19 = bitcast %"class.std::__cxx11::basic_stringbuf"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %19) #18
  %20 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !58
  %21 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 1
  %22 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 7
  %23 = bitcast i8** %21 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %23, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %22) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !58
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 1
  store i32 24, i32* %24, align 8, !tbaa !60
  %25 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2
  %26 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 2
  %27 = bitcast %"class.std::__cxx11::basic_string"* %25 to %union.anon**
  store %union.anon* %26, %union.anon** %27, align 8, !tbaa !65
  %28 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 1
  store i64 0, i64* %28, align 8, !tbaa !66
  %29 = bitcast %union.anon* %26 to i8*
  store i8 0, i8* %29, align 8, !tbaa !56
  %30 = bitcast %"class.std::basic_ostream"* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %30) #18
  %31 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0
  %32 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1
  %33 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %32, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %33) #18
  %34 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %32, i64 0, i32 0, i32 0
  %35 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %35, align 8, !tbaa !67
  %36 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 2
  store i8 0, i8* %36, align 8, !tbaa !70
  %37 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 3
  store i8 0, i8* %37, align 1, !tbaa !71
  %38 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 4
  %39 = bitcast %"class.std::basic_streambuf"** %38 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %39, i8 0, i64 32, i1 false) #18
  %40 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %40, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  %41 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %42 = getelementptr inbounds i8, i8* %30, i64 %41
  %43 = bitcast i8* %42 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %43, %"class.std::basic_streambuf"* nonnull %31)
          to label %46 unwind label %44

44:                                               ; preds = %17
  %45 = landingpad { i8*, i32 }
          cleanup
  br label %118

46:                                               ; preds = %17
  %47 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %5, i8* nonnull getelementptr inbounds ([29 x i8], [29 x i8]* @.str.12, i64 0, i64 0), i64 28)
          to label %48 unwind label %106

48:                                               ; preds = %46
  %49 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %5, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %50 unwind label %106

50:                                               ; preds = %48
  %51 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %5, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.13, i64 0, i64 0), i64 16)
          to label %52 unwind label %106

52:                                               ; preds = %50
  %53 = bitcast %"class.std::__cxx11::basic_string"* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %53) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !109)
  %54 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 2
  %55 = bitcast %"class.std::__cxx11::basic_string"* %6 to %union.anon**
  store %union.anon* %54, %union.anon** %55, align 8, !tbaa !65, !alias.scope !109
  %56 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 1
  store i64 0, i64* %56, align 8, !tbaa !66, !alias.scope !109
  %57 = bitcast %union.anon* %54 to i8*
  store i8 0, i8* %57, align 8, !tbaa !56, !alias.scope !109
  %58 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 5
  %59 = load i8*, i8** %58, align 8, !tbaa !75, !noalias !109
  %60 = icmp eq i8* %59, null
  br i1 %60, label %82, label %61

61:                                               ; preds = %52
  %62 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 3
  %63 = load i8*, i8** %62, align 8, !tbaa !78, !noalias !109
  %64 = icmp ugt i8* %59, %63
  %65 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 4
  %66 = load i8*, i8** %65, align 8, !tbaa !79
  br i1 %64, label %67, label %77

67:                                               ; preds = %61
  %68 = ptrtoint i8* %59 to i64
  %69 = ptrtoint i8* %66 to i64
  %70 = sub i64 %68, %69
  %71 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %6, i64 0, i64 0, i8* %66, i64 %70)
          to label %83 unwind label %72

72:                                               ; preds = %82, %77, %67
  %73 = landingpad { i8*, i32 }
          cleanup
  %74 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 0, i32 0
  %75 = load i8*, i8** %74, align 8, !tbaa !80, !alias.scope !109
  %76 = icmp eq i8* %75, %57
  br i1 %76, label %113, label %.sink.split

77:                                               ; preds = %61
  %78 = ptrtoint i8* %63 to i64
  %79 = ptrtoint i8* %66 to i64
  %80 = sub i64 %78, %79
  %81 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %6, i64 0, i64 0, i8* %66, i64 %80)
          to label %83 unwind label %72

82:                                               ; preds = %52
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %6, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %25)
          to label %83 unwind label %72

83:                                               ; preds = %82, %77, %67
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %6, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %3)
          to label %84 unwind label %108

84:                                               ; preds = %83
  %85 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 0, i32 0
  %86 = load i8*, i8** %85, align 8, !tbaa !80
  %87 = icmp eq i8* %86, %57
  br i1 %87, label %89, label %88

88:                                               ; preds = %84
  call void @_ZdlPv(i8* %86) #18
  br label %89

89:                                               ; preds = %88, %84
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %53) #18
  %90 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %90) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %30) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !58
  %91 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 0, i32 0
  %92 = load i8*, i8** %91, align 8, !tbaa !80
  %93 = icmp eq i8* %92, %29
  br i1 %93, label %95, label %94

94:                                               ; preds = %89
  call void @_ZdlPv(i8* %92) #18
  br label %95

95:                                               ; preds = %94, %89
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %22) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %19) #18
  %96 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 0, i32 0
  %97 = load i8*, i8** %96, align 8, !tbaa !80
  %98 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 2
  %99 = bitcast %union.anon* %98 to i8*
  %100 = icmp eq i8* %97, %99
  br i1 %100, label %102, label %101

101:                                              ; preds = %95
  call void @_ZdlPv(i8* %97) #18
  br label %102

102:                                              ; preds = %101, %95
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %18) #18
  %103 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7
  %104 = bitcast %"struct.std::atomic.0"* %103 to i64*
  %105 = atomicrmw xchg i64* %104, i64 0 seq_cst, align 8
  br label %133

106:                                              ; preds = %50, %48, %46
  %107 = landingpad { i8*, i32 }
          cleanup
  br label %115

108:                                              ; preds = %83
  %109 = landingpad { i8*, i32 }
          cleanup
  %110 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 0, i32 0
  %111 = load i8*, i8** %110, align 8, !tbaa !80
  %112 = icmp eq i8* %111, %57
  br i1 %112, label %113, label %.sink.split

.sink.split:                                      ; preds = %108, %72
  %.sink = phi i8* [ %75, %72 ], [ %111, %108 ]
  %.ph = phi { i8*, i32 } [ %73, %72 ], [ %109, %108 ]
  call void @_ZdlPv(i8* %.sink) #18
  br label %113

113:                                              ; preds = %.sink.split, %108, %72
  %114 = phi { i8*, i32 } [ %73, %72 ], [ %109, %108 ], [ %.ph, %.sink.split ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %53) #18
  br label %115

115:                                              ; preds = %113, %106
  %116 = phi { i8*, i32 } [ %114, %113 ], [ %107, %106 ]
  %117 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 0
  br label %118

118:                                              ; preds = %115, %44
  %119 = phi %"class.std::ios_base"* [ %33, %44 ], [ %117, %115 ]
  %120 = phi { i8*, i32 } [ %45, %44 ], [ %116, %115 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %119) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %30) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !58
  %121 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 0, i32 0
  %122 = load i8*, i8** %121, align 8, !tbaa !80
  %123 = icmp eq i8* %122, %29
  br i1 %123, label %125, label %124

124:                                              ; preds = %118
  call void @_ZdlPv(i8* %122) #18
  br label %125

125:                                              ; preds = %124, %118
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %22) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %19) #18
  %126 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 0, i32 0
  %127 = load i8*, i8** %126, align 8, !tbaa !80
  %128 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 2
  %129 = bitcast %union.anon* %128 to i8*
  %130 = icmp eq i8* %127, %129
  br i1 %130, label %132, label %131

131:                                              ; preds = %125
  call void @_ZdlPv(i8* %127) #18
  br label %132

132:                                              ; preds = %131, %125
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %18) #18
  br label %251

133:                                              ; preds = %102, %12
  %134 = phi i64 [ %105, %102 ], [ %16, %12 ]
  %135 = inttoptr i64 %134 to %"class.NES::Runtime::BufferRecycler"*
  %136 = icmp eq %"class.NES::Runtime::BufferRecycler"* %135, %1
  br i1 %136, label %137, label %250

137:                                              ; preds = %133
  %138 = bitcast %"class.std::__cxx11::basic_string"* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %138) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %7)
  %139 = bitcast %"class.std::__cxx11::basic_stringbuf"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %139) #18
  %140 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %141 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 1
  %142 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 7
  %143 = bitcast i8** %141 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %143, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %142) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %144 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 1
  store i32 24, i32* %144, align 8, !tbaa !60
  %145 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2
  %146 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 2
  %147 = bitcast %"class.std::__cxx11::basic_string"* %145 to %union.anon**
  store %union.anon* %146, %union.anon** %147, align 8, !tbaa !65
  %148 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 1
  store i64 0, i64* %148, align 8, !tbaa !66
  %149 = bitcast %union.anon* %146 to i8*
  store i8 0, i8* %149, align 8, !tbaa !56
  %150 = bitcast %"class.std::basic_ostream"* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %150) #18
  %151 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0
  %152 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1
  %153 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %152, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %153) #18
  %154 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %152, i64 0, i32 0, i32 0
  %155 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %155, align 8, !tbaa !67
  %156 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 2
  store i8 0, i8* %156, align 8, !tbaa !70
  %157 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 3
  store i8 0, i8* %157, align 1, !tbaa !71
  %158 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 4
  %159 = bitcast %"class.std::basic_streambuf"** %158 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %159, i8 0, i64 32, i1 false) #18
  %160 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %160, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %154, align 8, !tbaa !58
  %161 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %162 = getelementptr inbounds i8, i8* %150, i64 %161
  %163 = bitcast i8* %162 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %163, %"class.std::basic_streambuf"* nonnull %151)
          to label %166 unwind label %164

164:                                              ; preds = %137
  %165 = landingpad { i8*, i32 }
          cleanup
  br label %235

166:                                              ; preds = %137
  %167 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([44 x i8], [44 x i8]* @.str.14, i64 0, i64 0), i64 43)
          to label %168 unwind label %223

168:                                              ; preds = %166
  %169 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %170 unwind label %223

170:                                              ; preds = %168
  %171 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.13, i64 0, i64 0), i64 16)
          to label %172 unwind label %223

172:                                              ; preds = %170
  %173 = bitcast %"class.std::__cxx11::basic_string"* %10 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %173) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !112)
  %174 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 2
  %175 = bitcast %"class.std::__cxx11::basic_string"* %10 to %union.anon**
  store %union.anon* %174, %union.anon** %175, align 8, !tbaa !65, !alias.scope !112
  %176 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 1
  store i64 0, i64* %176, align 8, !tbaa !66, !alias.scope !112
  %177 = bitcast %union.anon* %174 to i8*
  store i8 0, i8* %177, align 8, !tbaa !56, !alias.scope !112
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 5
  %179 = load i8*, i8** %178, align 8, !tbaa !75, !noalias !112
  %180 = icmp eq i8* %179, null
  br i1 %180, label %202, label %181

181:                                              ; preds = %172
  %182 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 3
  %183 = load i8*, i8** %182, align 8, !tbaa !78, !noalias !112
  %184 = icmp ugt i8* %179, %183
  %185 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 4
  %186 = load i8*, i8** %185, align 8, !tbaa !79
  br i1 %184, label %187, label %197

187:                                              ; preds = %181
  %188 = ptrtoint i8* %179 to i64
  %189 = ptrtoint i8* %186 to i64
  %190 = sub i64 %188, %189
  %191 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, i64 0, i64 0, i8* %186, i64 %190)
          to label %203 unwind label %192

192:                                              ; preds = %202, %197, %187
  %193 = landingpad { i8*, i32 }
          cleanup
  %194 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %195 = load i8*, i8** %194, align 8, !tbaa !80, !alias.scope !112
  %196 = icmp eq i8* %195, %177
  br i1 %196, label %230, label %.sink.split19

197:                                              ; preds = %181
  %198 = ptrtoint i8* %183 to i64
  %199 = ptrtoint i8* %186 to i64
  %200 = sub i64 %198, %199
  %201 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, i64 0, i64 0, i8* %186, i64 %200)
          to label %203 unwind label %192

202:                                              ; preds = %172
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %145)
          to label %203 unwind label %192

203:                                              ; preds = %202, %197, %187
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %7)
          to label %204 unwind label %225

204:                                              ; preds = %203
  %205 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %206 = load i8*, i8** %205, align 8, !tbaa !80
  %207 = icmp eq i8* %206, %177
  br i1 %207, label %209, label %208

208:                                              ; preds = %204
  call void @_ZdlPv(i8* %206) #18
  br label %209

209:                                              ; preds = %208, %204
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %173) #18
  %210 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %210) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %150) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %211 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 0, i32 0
  %212 = load i8*, i8** %211, align 8, !tbaa !80
  %213 = icmp eq i8* %212, %149
  br i1 %213, label %215, label %214

214:                                              ; preds = %209
  call void @_ZdlPv(i8* %212) #18
  br label %215

215:                                              ; preds = %214, %209
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %142) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %139) #18
  %216 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %217 = load i8*, i8** %216, align 8, !tbaa !80
  %218 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %219 = bitcast %union.anon* %218 to i8*
  %220 = icmp eq i8* %217, %219
  br i1 %220, label %222, label %221

221:                                              ; preds = %215
  call void @_ZdlPv(i8* %217) #18
  br label %222

222:                                              ; preds = %221, %215
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %138) #18
  br label %250

223:                                              ; preds = %170, %168, %166
  %224 = landingpad { i8*, i32 }
          cleanup
  br label %232

225:                                              ; preds = %203
  %226 = landingpad { i8*, i32 }
          cleanup
  %227 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %228 = load i8*, i8** %227, align 8, !tbaa !80
  %229 = icmp eq i8* %228, %177
  br i1 %229, label %230, label %.sink.split19

.sink.split19:                                    ; preds = %225, %192
  %.sink21 = phi i8* [ %195, %192 ], [ %228, %225 ]
  %.ph20 = phi { i8*, i32 } [ %193, %192 ], [ %226, %225 ]
  call void @_ZdlPv(i8* %.sink21) #18
  br label %230

230:                                              ; preds = %.sink.split19, %225, %192
  %231 = phi { i8*, i32 } [ %193, %192 ], [ %226, %225 ], [ %.ph20, %.sink.split19 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %173) #18
  br label %232

232:                                              ; preds = %230, %223
  %233 = phi { i8*, i32 } [ %231, %230 ], [ %224, %223 ]
  %234 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 0
  br label %235

235:                                              ; preds = %232, %164
  %236 = phi %"class.std::ios_base"* [ %153, %164 ], [ %234, %232 ]
  %237 = phi { i8*, i32 } [ %165, %164 ], [ %233, %232 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %236) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %150) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  %238 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 0, i32 0
  %239 = load i8*, i8** %238, align 8, !tbaa !80
  %240 = icmp eq i8* %239, %149
  br i1 %240, label %242, label %241

241:                                              ; preds = %235
  call void @_ZdlPv(i8* %239) #18
  br label %242

242:                                              ; preds = %241, %235
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %140, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %142) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %139) #18
  %243 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %244 = load i8*, i8** %243, align 8, !tbaa !80
  %245 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %246 = bitcast %union.anon* %245 to i8*
  %247 = icmp eq i8* %244, %246
  br i1 %247, label %249, label %248

248:                                              ; preds = %242
  call void @_ZdlPv(i8* %244) #18
  br label %249

249:                                              ; preds = %248, %242
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %138) #18
  br label %251

250:                                              ; preds = %222, %133
  ret void

251:                                              ; preds = %249, %132
  %252 = phi { i8*, i32 } [ %237, %249 ], [ %120, %132 ]
  resume { i8*, i32 } %252
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEE(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %0, %"class.std::function"* nonnull align 8 dereferenceable(32) %1) local_unnamed_addr #11 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %3 = alloca { i64, i64 }, align 8
  %4 = alloca %"class.std::function", align 8
  %5 = alloca %class.anon, align 8
  %6 = alloca %"class.std::function", align 8
  %7 = alloca %class.anon, align 8
  %8 = bitcast %"class.std::function"* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %8) #18
  %9 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %10 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  %11 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  %12 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !53
  %13 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %12, null
  br i1 %13, label %14, label %17

14:                                               ; preds = %2
  %15 = bitcast %class.anon* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 64, i8* nonnull %15) #18
  %16 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %16, align 8, !tbaa !53
  br label %54

17:                                               ; preds = %2
  %18 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 0, i32 0
  %19 = getelementptr inbounds %"class.std::function", %"class.std::function"* %9, i64 0, i32 0, i32 0
  %20 = invoke zeroext i1 %12(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %19, i32 2)
          to label %30 unwind label %21

21:                                               ; preds = %17
  %22 = landingpad { i8*, i32 }
          catch i8* null
  %23 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  %24 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %23, null
  br i1 %24, label %211, label %25

25:                                               ; preds = %21
  %26 = invoke zeroext i1 %23(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, i32 3)
          to label %211 unwind label %27

27:                                               ; preds = %25
  %28 = landingpad { i8*, i32 }
          catch i8* null
  %29 = extractvalue { i8*, i32 } %28, 0
  call void @__clang_call_terminate(i8* %29) #21
  unreachable

30:                                               ; preds = %17
  %31 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %32 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %31, align 8, !tbaa !105
  %33 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %32, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %33, align 8, !tbaa !105
  %34 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !53
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %34, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  %35 = bitcast %class.anon* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 64, i8* nonnull %35) #18
  %36 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %36, align 8, !tbaa !53
  %37 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %34, null
  br i1 %37, label %54, label %38

38:                                               ; preds = %30
  %39 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 0
  %40 = invoke zeroext i1 %34(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %39, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, i32 2)
          to label %41 unwind label %45

41:                                               ; preds = %38
  %42 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %33, align 8, !tbaa !105
  %43 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %42, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %43, align 8, !tbaa !105
  %44 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %44, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %36, align 8, !tbaa !53
  br label %54

45:                                               ; preds = %38
  %46 = landingpad { i8*, i32 }
          catch i8* null
  %47 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %36, align 8, !tbaa !53
  %48 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %47, null
  br i1 %48, label %201, label %49

49:                                               ; preds = %45
  %50 = invoke zeroext i1 %47(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %39, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %39, i32 3)
          to label %201 unwind label %51

51:                                               ; preds = %49
  %52 = landingpad { i8*, i32 }
          catch i8* null
  %53 = extractvalue { i8*, i32 } %52, 0
  call void @__clang_call_terminate(i8* %53) #21
  unreachable

54:                                               ; preds = %41, %30, %14
  %55 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ null, %14 ], [ null, %30 ], [ %44, %41 ]
  %.pre-phi = bitcast %class.anon* %7 to i8*
  %56 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1
  %57 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %57, align 8, !tbaa !53
  %58 = getelementptr inbounds %"class.std::function", %"class.std::function"* %1, i64 0, i32 0, i32 1
  %59 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %58, align 8, !tbaa !53
  %60 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %59, null
  br i1 %60, label %61, label %64

61:                                               ; preds = %54
  %62 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 1
  %63 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %62, align 8, !tbaa !57
  br label %84

64:                                               ; preds = %54
  %65 = getelementptr inbounds %"class.std::function", %"class.std::function"* %56, i64 0, i32 0, i32 0
  %66 = getelementptr inbounds %"class.std::function", %"class.std::function"* %1, i64 0, i32 0, i32 0
  %67 = invoke zeroext i1 %59(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %65, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %66, i32 2)
          to label %68 unwind label %75

68:                                               ; preds = %64
  %69 = getelementptr inbounds %"class.std::function", %"class.std::function"* %1, i64 0, i32 1
  %70 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %69, align 8, !tbaa !105
  %71 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %70, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %71, align 8, !tbaa !105
  %72 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %58, align 8, !tbaa !53
  %73 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 1
  %74 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %73, align 8, !tbaa !57
  br label %84

75:                                               ; preds = %64
  %76 = landingpad { i8*, i32 }
          catch i8* null
  %77 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %57, align 8, !tbaa !53
  %78 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %77, null
  br i1 %78, label %176, label %79

79:                                               ; preds = %75
  %80 = invoke zeroext i1 %77(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %65, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %65, i32 3)
          to label %176 unwind label %81

81:                                               ; preds = %79
  %82 = landingpad { i8*, i32 }
          catch i8* null
  %83 = extractvalue { i8*, i32 } %82, 0
  call void @__clang_call_terminate(i8* %83) #21
  unreachable

84:                                               ; preds = %68, %61
  %85 = phi void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* [ %70, %68 ], [ %63, %61 ]
  %86 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ %72, %68 ], [ null, %61 ]
  %87 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ %74, %68 ], [ %55, %61 ]
  %88 = bitcast %class.anon* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 64, i8* nonnull %88)
  %89 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %89) #18
  %90 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 0, i32 0, i32 1
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %88, i8* noundef nonnull align 8 dereferenceable(16) %.pre-phi, i64 16, i1 false) #18
  %91 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %91, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %87, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %90, align 8, !tbaa !57
  %92 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 1
  %93 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 0, i32 1
  %94 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %92, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %94, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %93, align 8, !tbaa !57
  %95 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 1
  %96 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 1, i32 0, i32 1
  %97 = bitcast %"class.std::function"* %56 to i8*
  %98 = bitcast %"class.std::function"* %95 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %98, i8* noundef nonnull align 8 dereferenceable(16) %97, i64 16, i1 false) #18
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %57, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %86, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %96, align 8, !tbaa !57
  %99 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 1, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %85, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %99, align 8, !tbaa !57
  %100 = invoke noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #20
          to label %101 unwind label %126

101:                                              ; preds = %84
  %102 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  %103 = getelementptr inbounds i8, i8* %100, i64 16
  %104 = bitcast i8* %103 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %88, i8* noundef nonnull align 8 dereferenceable(16) %100, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %100, i8* noundef nonnull align 8 dereferenceable(16) %.pre-phi, i64 16, i1 false)
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %90, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %87, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %104, align 8, !tbaa !57
  %105 = getelementptr inbounds i8, i8* %100, i64 24
  %106 = bitcast i8* %105 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %94, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %106, align 8, !tbaa !57
  %107 = getelementptr inbounds i8, i8* %100, i64 32
  %108 = getelementptr inbounds i8, i8* %100, i64 48
  %109 = bitcast i8* %108 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %98, i8* noundef nonnull align 8 dereferenceable(16) %107, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %107, i8* noundef nonnull align 8 dereferenceable(16) %97, i64 16, i1 false)
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %86, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %109, align 8, !tbaa !57
  %110 = getelementptr inbounds i8, i8* %100, i64 56
  %111 = bitcast i8* %110 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %85, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %111, align 8, !tbaa !57
  %112 = bitcast %"class.std::function"* %4 to i8**
  store i8* %100, i8** %112, align 8, !tbaa !57
  %113 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %114 = bitcast { i64, i64 }* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %114)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %114, i8* noundef nonnull align 8 dereferenceable(16) %89, i64 16, i1 false) #18, !tbaa.struct !55
  %115 = bitcast %"class.std::function"* %9 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %89, i8* noundef nonnull align 8 dereferenceable(16) %115, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(16) %115, i8* noundef nonnull align 8 dereferenceable(16) %114, i64 16, i1 false) #18, !tbaa.struct !55
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %114)
  %116 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %116, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %102, align 8, !tbaa !57
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* @"_ZNSt14_Function_base13_Base_managerIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS3_13MemorySegmentEPNS2_14BufferRecyclerEEEE3$_0E10_M_managerERSt9_Any_dataRKSF_St18_Manager_operation", i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !57
  %117 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %118 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %117, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %118, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %113, align 8, !tbaa !57
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* @"_ZNSt17_Function_handlerIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEZNS2_18BufferControlBlock18addRecycleCallbackEOSt8functionIS7_EE3$_0E9_M_invokeERKSt9_Any_dataOS4_OS6_", void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %117, align 8, !tbaa !57
  %119 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %116, null
  br i1 %119, label %149, label %120

120:                                              ; preds = %101
  %121 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %122 = invoke zeroext i1 %116(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %121, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %121, i32 3)
          to label %149 unwind label %123

123:                                              ; preds = %120
  %124 = landingpad { i8*, i32 }
          catch i8* null
  %125 = extractvalue { i8*, i32 } %124, 0
  call void @__clang_call_terminate(i8* %125) #21
  unreachable

126:                                              ; preds = %84
  %127 = landingpad { i8*, i32 }
          catch i8* null
  %128 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %86, null
  br i1 %128, label %137, label %129

129:                                              ; preds = %126
  %130 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 1, i32 0, i32 0
  %131 = invoke zeroext i1 %86(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %130, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %130, i32 3)
          to label %132 unwind label %134

132:                                              ; preds = %129
  %133 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %90, align 8, !tbaa !53
  br label %137

134:                                              ; preds = %129
  %135 = landingpad { i8*, i32 }
          catch i8* null
  %136 = extractvalue { i8*, i32 } %135, 0
  call void @__clang_call_terminate(i8* %136) #21
  unreachable

137:                                              ; preds = %132, %126
  %138 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ %133, %132 ], [ %87, %126 ]
  %139 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %138, null
  br i1 %139, label %146, label %140

140:                                              ; preds = %137
  %141 = getelementptr inbounds %class.anon, %class.anon* %5, i64 0, i32 0, i32 0, i32 0
  %142 = invoke zeroext i1 %138(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %141, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %141, i32 3)
          to label %146 unwind label %143

143:                                              ; preds = %140
  %144 = landingpad { i8*, i32 }
          catch i8* null
  %145 = extractvalue { i8*, i32 } %144, 0
  call void @__clang_call_terminate(i8* %145) #21
  unreachable

146:                                              ; preds = %140, %137
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %89) #18
  %147 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %57, align 8, !tbaa !53
  %148 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %147, null
  br i1 %148, label %192, label %186

149:                                              ; preds = %120, %101
  %.pre = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %57, align 8, !tbaa !53
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %89) #18
  call void @llvm.lifetime.end.p0i8(i64 64, i8* nonnull %88)
  %150 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %.pre, null
  br i1 %150, label %157, label %151

151:                                              ; preds = %149
  %152 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 0, i32 0
  %153 = invoke zeroext i1 %.pre(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %152, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %152, i32 3)
          to label %157 unwind label %154

154:                                              ; preds = %151
  %155 = landingpad { i8*, i32 }
          catch i8* null
  %156 = extractvalue { i8*, i32 } %155, 0
  call void @__clang_call_terminate(i8* %156) #21
  unreachable

157:                                              ; preds = %151, %149
  %158 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %91, align 8, !tbaa !53
  %159 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %158, null
  br i1 %159, label %166, label %160

160:                                              ; preds = %157
  %161 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 0
  %162 = invoke zeroext i1 %158(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %161, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %161, i32 3)
          to label %166 unwind label %163

163:                                              ; preds = %160
  %164 = landingpad { i8*, i32 }
          catch i8* null
  %165 = extractvalue { i8*, i32 } %164, 0
  call void @__clang_call_terminate(i8* %165) #21
  unreachable

166:                                              ; preds = %160, %157
  call void @llvm.lifetime.end.p0i8(i64 64, i8* nonnull %.pre-phi) #18
  %167 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  %168 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %167, null
  br i1 %168, label %175, label %169

169:                                              ; preds = %166
  %170 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 0, i32 0
  %171 = invoke zeroext i1 %167(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %170, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %170, i32 3)
          to label %175 unwind label %172

172:                                              ; preds = %169
  %173 = landingpad { i8*, i32 }
          catch i8* null
  %174 = extractvalue { i8*, i32 } %173, 0
  call void @__clang_call_terminate(i8* %174) #21
  unreachable

175:                                              ; preds = %169, %166
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %8) #18
  ret void

176:                                              ; preds = %79, %75
  %177 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 1
  %178 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %177, align 8, !tbaa !53
  %179 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %178, null
  br i1 %179, label %201, label %180

180:                                              ; preds = %176
  %181 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 0
  %182 = invoke zeroext i1 %178(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %181, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %181, i32 3)
          to label %201 unwind label %183

183:                                              ; preds = %180
  %184 = landingpad { i8*, i32 }
          catch i8* null
  %185 = extractvalue { i8*, i32 } %184, 0
  call void @__clang_call_terminate(i8* %185) #21
  unreachable

186:                                              ; preds = %146
  %187 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 0, i32 0
  %188 = invoke zeroext i1 %147(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %187, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %187, i32 3)
          to label %192 unwind label %189

189:                                              ; preds = %186
  %190 = landingpad { i8*, i32 }
          catch i8* null
  %191 = extractvalue { i8*, i32 } %190, 0
  call void @__clang_call_terminate(i8* %191) #21
  unreachable

192:                                              ; preds = %186, %146
  %193 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %91, align 8, !tbaa !53
  %194 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %193, null
  br i1 %194, label %201, label %195

195:                                              ; preds = %192
  %196 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 0
  %197 = invoke zeroext i1 %193(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %196, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %196, i32 3)
          to label %201 unwind label %198

198:                                              ; preds = %195
  %199 = landingpad { i8*, i32 }
          catch i8* null
  %200 = extractvalue { i8*, i32 } %199, 0
  call void @__clang_call_terminate(i8* %200) #21
  unreachable

201:                                              ; preds = %195, %192, %180, %176, %49, %45
  %202 = phi { i8*, i32 } [ %127, %195 ], [ %127, %192 ], [ %76, %180 ], [ %76, %176 ], [ %46, %49 ], [ %46, %45 ]
  %.pre-phi35 = bitcast %class.anon* %7 to i8*
  call void @llvm.lifetime.end.p0i8(i64 64, i8* nonnull %.pre-phi35) #18
  %203 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !53
  %204 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %203, null
  br i1 %204, label %211, label %205

205:                                              ; preds = %201
  %206 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 0, i32 0
  %207 = invoke zeroext i1 %203(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %206, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %206, i32 3)
          to label %211 unwind label %208

208:                                              ; preds = %205
  %209 = landingpad { i8*, i32 }
          catch i8* null
  %210 = extractvalue { i8*, i32 } %209, 0
  call void @__clang_call_terminate(i8* %210) #21
  unreachable

211:                                              ; preds = %205, %201, %25, %21
  %212 = phi { i8*, i32 } [ %22, %25 ], [ %22, %21 ], [ %202, %201 ], [ %202, %205 ]
  %213 = extractvalue { i8*, i32 } %212, 0
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %8) #18
  call void @__clang_call_terminate(i8* %213) #21
  unreachable
}

; Function Attrs: uwtable
define internal zeroext i1 @"_ZNSt14_Function_base13_Base_managerIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS3_13MemorySegmentEPNS2_14BufferRecyclerEEEE3$_0E10_M_managerERSt9_Any_dataRKSF_St18_Manager_operation"(%"union.std::_Any_data"* nocapture nonnull align 8 dereferenceable(16) %0, %"union.std::_Any_data"* nocapture nonnull readonly align 8 dereferenceable(16) %1, i32 %2) #4 align 2 personality i32 (...)* @__gxx_personality_v0 {
  switch i32 %2, label %103 [
    i32 0, label %4
    i32 1, label %6
    i32 2, label %10
    i32 3, label %77
  ]

4:                                                ; preds = %3
  %5 = bitcast %"union.std::_Any_data"* %0 to %"class.std::type_info"**
  store %"class.std::type_info"* bitcast ({ i8*, i8* }* @"_ZTIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEEE3$_0" to %"class.std::type_info"*), %"class.std::type_info"** %5, align 8, !tbaa !57
  br label %103

6:                                                ; preds = %3
  %7 = bitcast %"union.std::_Any_data"* %1 to %class.anon**
  %8 = load %class.anon*, %class.anon** %7, align 8, !tbaa !57
  %9 = bitcast %"union.std::_Any_data"* %0 to %class.anon**
  store %class.anon* %8, %class.anon** %9, align 8, !tbaa !57
  br label %103

10:                                               ; preds = %3
  %11 = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #20
  %12 = bitcast %"union.std::_Any_data"* %1 to %class.anon**
  %13 = load %class.anon*, %class.anon** %12, align 8, !tbaa !57
  %14 = getelementptr inbounds i8, i8* %11, i64 16
  %15 = bitcast i8* %14 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !53
  %16 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 0, i32 0, i32 1
  %17 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %16, align 8, !tbaa !53
  %18 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %17, null
  br i1 %18, label %38, label %19

19:                                               ; preds = %10
  %20 = bitcast i8* %11 to %"union.std::_Any_data"*
  %21 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 0, i32 0, i32 0
  %22 = invoke zeroext i1 %17(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %21, i32 2)
          to label %23 unwind label %29

23:                                               ; preds = %19
  %24 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 0, i32 1
  %25 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %24, align 8, !tbaa !105
  %26 = getelementptr inbounds i8, i8* %11, i64 24
  %27 = bitcast i8* %26 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %25, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %27, align 8, !tbaa !105
  %28 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %16, align 8, !tbaa !53
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %28, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !53
  br label %38

29:                                               ; preds = %19
  %30 = landingpad { i8*, i32 }
          cleanup
  %31 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !53
  %32 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %31, null
  br i1 %32, label %73, label %33

33:                                               ; preds = %29
  %34 = invoke zeroext i1 %31(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, i32 3)
          to label %73 unwind label %35

35:                                               ; preds = %33
  %36 = landingpad { i8*, i32 }
          catch i8* null
  %37 = extractvalue { i8*, i32 } %36, 0
  tail call void @__clang_call_terminate(i8* %37) #21
  unreachable

38:                                               ; preds = %23, %10
  %39 = getelementptr inbounds i8, i8* %11, i64 48
  %40 = bitcast i8* %39 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !53
  %41 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 1, i32 0, i32 1
  %42 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %41, align 8, !tbaa !53
  %43 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %42, null
  br i1 %43, label %75, label %44

44:                                               ; preds = %38
  %45 = getelementptr inbounds i8, i8* %11, i64 32
  %46 = bitcast i8* %45 to %"union.std::_Any_data"*
  %47 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 1, i32 0, i32 0
  %48 = invoke zeroext i1 %42(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %46, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %47, i32 2)
          to label %49 unwind label %55

49:                                               ; preds = %44
  %50 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 1, i32 1
  %51 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %50, align 8, !tbaa !105
  %52 = getelementptr inbounds i8, i8* %11, i64 56
  %53 = bitcast i8* %52 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %51, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %53, align 8, !tbaa !105
  %54 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %41, align 8, !tbaa !53
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %54, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !53
  br label %75

55:                                               ; preds = %44
  %56 = landingpad { i8*, i32 }
          cleanup
  %57 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !53
  %58 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %57, null
  br i1 %58, label %64, label %59

59:                                               ; preds = %55
  %60 = invoke zeroext i1 %57(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %46, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %46, i32 3)
          to label %64 unwind label %61

61:                                               ; preds = %59
  %62 = landingpad { i8*, i32 }
          catch i8* null
  %63 = extractvalue { i8*, i32 } %62, 0
  tail call void @__clang_call_terminate(i8* %63) #21
  unreachable

64:                                               ; preds = %59, %55
  %65 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !53
  %66 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %65, null
  br i1 %66, label %73, label %67

67:                                               ; preds = %64
  %68 = bitcast i8* %11 to %"union.std::_Any_data"*
  %69 = invoke zeroext i1 %65(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %68, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %68, i32 3)
          to label %73 unwind label %70

70:                                               ; preds = %67
  %71 = landingpad { i8*, i32 }
          catch i8* null
  %72 = extractvalue { i8*, i32 } %71, 0
  tail call void @__clang_call_terminate(i8* %72) #21
  unreachable

73:                                               ; preds = %67, %64, %33, %29
  %74 = phi { i8*, i32 } [ %30, %33 ], [ %30, %29 ], [ %56, %67 ], [ %56, %64 ]
  tail call void @_ZdlPv(i8* nonnull %11) #22
  resume { i8*, i32 } %74

75:                                               ; preds = %49, %38
  %76 = bitcast %"union.std::_Any_data"* %0 to i8**
  store i8* %11, i8** %76, align 8, !tbaa !57
  br label %103

77:                                               ; preds = %3
  %78 = bitcast %"union.std::_Any_data"* %0 to %class.anon**
  %79 = load %class.anon*, %class.anon** %78, align 8, !tbaa !57
  %80 = icmp eq %class.anon* %79, null
  br i1 %80, label %103, label %81

81:                                               ; preds = %77
  %82 = getelementptr inbounds %class.anon, %class.anon* %79, i64 0, i32 1, i32 0, i32 1
  %83 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %82, align 8, !tbaa !53
  %84 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %83, null
  br i1 %84, label %91, label %85

85:                                               ; preds = %81
  %86 = getelementptr inbounds %class.anon, %class.anon* %79, i64 0, i32 1, i32 0, i32 0
  %87 = invoke zeroext i1 %83(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %86, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %86, i32 3)
          to label %91 unwind label %88

88:                                               ; preds = %85
  %89 = landingpad { i8*, i32 }
          catch i8* null
  %90 = extractvalue { i8*, i32 } %89, 0
  tail call void @__clang_call_terminate(i8* %90) #21
  unreachable

91:                                               ; preds = %85, %81
  %92 = getelementptr inbounds %class.anon, %class.anon* %79, i64 0, i32 0, i32 0, i32 1
  %93 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %92, align 8, !tbaa !53
  %94 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %93, null
  br i1 %94, label %101, label %95

95:                                               ; preds = %91
  %96 = getelementptr inbounds %class.anon, %class.anon* %79, i64 0, i32 0, i32 0, i32 0
  %97 = invoke zeroext i1 %93(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %96, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %96, i32 3)
          to label %101 unwind label %98

98:                                               ; preds = %95
  %99 = landingpad { i8*, i32 }
          catch i8* null
  %100 = extractvalue { i8*, i32 } %99, 0
  tail call void @__clang_call_terminate(i8* %100) #21
  unreachable

101:                                              ; preds = %95, %91
  %102 = bitcast %class.anon* %79 to i8*
  tail call void @_ZdlPv(i8* %102) #22
  br label %103

103:                                              ; preds = %101, %77, %75, %6, %4, %3
  ret i1 false
}

; Function Attrs: mustprogress uwtable
define internal void @"_ZNSt17_Function_handlerIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEZNS2_18BufferControlBlock18addRecycleCallbackEOSt8functionIS7_EE3$_0E9_M_invokeERKSt9_Any_dataOS4_OS6_"(%"union.std::_Any_data"* nocapture nonnull readonly align 8 dereferenceable(16) %0, %"class.NES::Runtime::detail::MemorySegment"** nocapture nonnull readonly align 8 dereferenceable(8) %1, %"class.NES::Runtime::BufferRecycler"** nocapture nonnull readonly align 8 dereferenceable(8) %2) #15 align 2 {
  %4 = alloca %"class.NES::Runtime::detail::MemorySegment"*, align 8
  %5 = alloca %"class.NES::Runtime::BufferRecycler"*, align 8
  %6 = alloca %"class.NES::Runtime::detail::MemorySegment"*, align 8
  %7 = alloca %"class.NES::Runtime::BufferRecycler"*, align 8
  %8 = bitcast %"union.std::_Any_data"* %0 to %class.anon**
  %9 = load %class.anon*, %class.anon** %8, align 8, !tbaa !57
  %10 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %1, align 8, !tbaa !57
  %11 = load %"class.NES::Runtime::BufferRecycler"*, %"class.NES::Runtime::BufferRecycler"** %2, align 8, !tbaa !57
  %12 = bitcast %"class.NES::Runtime::detail::MemorySegment"** %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %12)
  %13 = bitcast %"class.NES::Runtime::BufferRecycler"** %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %13)
  store %"class.NES::Runtime::detail::MemorySegment"* %10, %"class.NES::Runtime::detail::MemorySegment"** %6, align 8, !tbaa !57
  store %"class.NES::Runtime::BufferRecycler"* %11, %"class.NES::Runtime::BufferRecycler"** %7, align 8, !tbaa !57
  %14 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 1
  %15 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !53
  %16 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %15, null
  br i1 %16, label %17, label %18

17:                                               ; preds = %3
  tail call void @_ZSt25__throw_bad_function_callv() #19
  unreachable

18:                                               ; preds = %3
  %19 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 1
  %20 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %19, align 8, !tbaa !105
  %21 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 0
  call void %20(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %21, %"class.NES::Runtime::detail::MemorySegment"** nonnull align 8 dereferenceable(8) %6, %"class.NES::Runtime::BufferRecycler"** nonnull align 8 dereferenceable(8) %7)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %12)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %13)
  %22 = bitcast %"class.NES::Runtime::detail::MemorySegment"** %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %22)
  %23 = bitcast %"class.NES::Runtime::BufferRecycler"** %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %23)
  store %"class.NES::Runtime::detail::MemorySegment"* %10, %"class.NES::Runtime::detail::MemorySegment"** %4, align 8, !tbaa !57
  store %"class.NES::Runtime::BufferRecycler"* %11, %"class.NES::Runtime::BufferRecycler"** %5, align 8, !tbaa !57
  %24 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  %25 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %24, align 8, !tbaa !53
  %26 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %25, null
  br i1 %26, label %27, label %28

27:                                               ; preds = %18
  call void @_ZSt25__throw_bad_function_callv() #19
  unreachable

28:                                               ; preds = %18
  %29 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 1
  %30 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %29, align 8, !tbaa !105
  %31 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 0
  call void %30(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %31, %"class.NES::Runtime::detail::MemorySegment"** nonnull align 8 dereferenceable(8) %4, %"class.NES::Runtime::BufferRecycler"** nonnull align 8 dereferenceable(8) %5)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %22)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %23)
  ret void
}

; Function Attrs: noreturn
declare dso_local void @_ZSt25__throw_bad_function_callv() local_unnamed_addr #16

; Function Attrs: mustprogress nofree norecurse nounwind uwtable willreturn
define dso_local nonnull %"class.NES::Runtime::detail::BufferControlBlock"* @_ZN3NES7Runtime6detail18BufferControlBlock6retainEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull returned dereferenceable(88) %0) local_unnamed_addr #13 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %3 = atomicrmw add i32* %2, i32 1 seq_cst, align 4
  ret %"class.NES::Runtime::detail::BufferControlBlock"* %0
}

; Function Attrs: uwtable
define dso_local zeroext i1 @_ZN3NES7Runtime6detail18BufferControlBlock7releaseEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %0) local_unnamed_addr #4 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %2 = alloca %"class.NES::Runtime::detail::MemorySegment"*, align 8
  %3 = alloca %"class.NES::Runtime::BufferRecycler"*, align 8
  %4 = alloca %"class.std::__cxx11::basic_string", align 8
  %5 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %6 = alloca %"class.std::basic_ostream", align 8
  %7 = alloca %"class.std::__cxx11::basic_string", align 8
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %9 = atomicrmw sub i32* %8, i32 1 seq_cst, align 4
  %10 = icmp eq i32 %9, 1
  br i1 %10, label %11, label %29

11:                                               ; preds = %1
  %12 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  store i32 0, i32* %12, align 4, !tbaa !17
  %13 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  %14 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %13, align 8, !tbaa !51
  %15 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7
  %16 = bitcast %"struct.std::atomic.0"* %15 to i64*
  %17 = load atomic i64, i64* %16 seq_cst, align 8
  %18 = inttoptr i64 %17 to %"class.NES::Runtime::BufferRecycler"*
  %19 = bitcast %"class.NES::Runtime::detail::MemorySegment"** %2 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %19)
  %20 = bitcast %"class.NES::Runtime::BufferRecycler"** %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %20)
  store %"class.NES::Runtime::detail::MemorySegment"* %14, %"class.NES::Runtime::detail::MemorySegment"** %2, align 8, !tbaa !57
  store %"class.NES::Runtime::BufferRecycler"* %18, %"class.NES::Runtime::BufferRecycler"** %3, align 8, !tbaa !57
  %21 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  %22 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %21, align 8, !tbaa !53
  %23 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %22, null
  br i1 %23, label %24, label %25

24:                                               ; preds = %11
  tail call void @_ZSt25__throw_bad_function_callv() #19
  unreachable

25:                                               ; preds = %11
  %26 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %27 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %26, align 8, !tbaa !105
  %28 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 0
  call void %27(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %28, %"class.NES::Runtime::detail::MemorySegment"** nonnull align 8 dereferenceable(8) %2, %"class.NES::Runtime::BufferRecycler"** nonnull align 8 dereferenceable(8) %3)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %19)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %20)
  br label %144

29:                                               ; preds = %1
  %30 = icmp eq i32 %9, 0
  br i1 %30, label %31, label %144

31:                                               ; preds = %29
  %32 = bitcast %"class.std::__cxx11::basic_string"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %32) #18
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %4)
  %33 = bitcast %"class.std::__cxx11::basic_stringbuf"* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %33) #18
  %34 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  %35 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 1
  %36 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 7
  %37 = bitcast i8** %35 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(48) %37, i8 0, i64 48, i1 false) #18
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %36) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  %38 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 1
  store i32 24, i32* %38, align 8, !tbaa !60
  %39 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2
  %40 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 2
  %41 = bitcast %"class.std::__cxx11::basic_string"* %39 to %union.anon**
  store %union.anon* %40, %union.anon** %41, align 8, !tbaa !65
  %42 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 1
  store i64 0, i64* %42, align 8, !tbaa !66
  %43 = bitcast %union.anon* %40 to i8*
  store i8 0, i8* %43, align 8, !tbaa !56
  %44 = bitcast %"class.std::basic_ostream"* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %44) #18
  %45 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0
  %46 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1
  %47 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %46, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %47) #18
  %48 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %46, i64 0, i32 0, i32 0
  %49 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %49, align 8, !tbaa !67
  %50 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 2
  store i8 0, i8* %50, align 8, !tbaa !70
  %51 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 3
  store i8 0, i8* %51, align 1, !tbaa !71
  %52 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 4
  %53 = bitcast %"class.std::basic_streambuf"** %52 to i8*
  call void @llvm.memset.p0i8.i64(i8* noundef nonnull align 8 dereferenceable(32) %53, i8 0, i64 32, i1 false) #18
  %54 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %54, align 8, !tbaa !58
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %48, align 8, !tbaa !58
  %55 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %56 = getelementptr inbounds i8, i8* %44, i64 %55
  %57 = bitcast i8* %56 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %57, %"class.std::basic_streambuf"* nonnull %45)
          to label %60 unwind label %58

58:                                               ; preds = %31
  %59 = landingpad { i8*, i32 }
          cleanup
  br label %129

60:                                               ; preds = %31
  %61 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %6, i8* nonnull getelementptr inbounds ([36 x i8], [36 x i8]* @.str.15, i64 0, i64 0), i64 35)
          to label %62 unwind label %117

62:                                               ; preds = %60
  %63 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %6, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %64 unwind label %117

64:                                               ; preds = %62
  %65 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %6, i8* nonnull getelementptr inbounds ([57 x i8], [57 x i8]* @.str.16, i64 0, i64 0), i64 56)
          to label %66 unwind label %117

66:                                               ; preds = %64
  %67 = bitcast %"class.std::__cxx11::basic_string"* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %67) #18
  call void @llvm.experimental.noalias.scope.decl(metadata !115)
  %68 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %69 = bitcast %"class.std::__cxx11::basic_string"* %7 to %union.anon**
  store %union.anon* %68, %union.anon** %69, align 8, !tbaa !65, !alias.scope !115
  %70 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 1
  store i64 0, i64* %70, align 8, !tbaa !66, !alias.scope !115
  %71 = bitcast %union.anon* %68 to i8*
  store i8 0, i8* %71, align 8, !tbaa !56, !alias.scope !115
  %72 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 5
  %73 = load i8*, i8** %72, align 8, !tbaa !75, !noalias !115
  %74 = icmp eq i8* %73, null
  br i1 %74, label %96, label %75

75:                                               ; preds = %66
  %76 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 3
  %77 = load i8*, i8** %76, align 8, !tbaa !78, !noalias !115
  %78 = icmp ugt i8* %73, %77
  %79 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 4
  %80 = load i8*, i8** %79, align 8, !tbaa !79
  br i1 %78, label %81, label %91

81:                                               ; preds = %75
  %82 = ptrtoint i8* %73 to i64
  %83 = ptrtoint i8* %80 to i64
  %84 = sub i64 %82, %83
  %85 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %7, i64 0, i64 0, i8* %80, i64 %84)
          to label %97 unwind label %86

86:                                               ; preds = %96, %91, %81
  %87 = landingpad { i8*, i32 }
          cleanup
  %88 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %89 = load i8*, i8** %88, align 8, !tbaa !80, !alias.scope !115
  %90 = icmp eq i8* %89, %71
  br i1 %90, label %124, label %.sink.split

91:                                               ; preds = %75
  %92 = ptrtoint i8* %77 to i64
  %93 = ptrtoint i8* %80 to i64
  %94 = sub i64 %92, %93
  %95 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %7, i64 0, i64 0, i8* %80, i64 %94)
          to label %97 unwind label %86

96:                                               ; preds = %66
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %7, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %39)
          to label %97 unwind label %86

97:                                               ; preds = %96, %91, %81
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %7, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %4)
          to label %98 unwind label %119

98:                                               ; preds = %97
  %99 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %100 = load i8*, i8** %99, align 8, !tbaa !80
  %101 = icmp eq i8* %100, %71
  br i1 %101, label %103, label %102

102:                                              ; preds = %98
  call void @_ZdlPv(i8* %100) #18
  br label %103

103:                                              ; preds = %102, %98
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %67) #18
  %104 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %104) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %44) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  %105 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 0, i32 0
  %106 = load i8*, i8** %105, align 8, !tbaa !80
  %107 = icmp eq i8* %106, %43
  br i1 %107, label %109, label %108

108:                                              ; preds = %103
  call void @_ZdlPv(i8* %106) #18
  br label %109

109:                                              ; preds = %108, %103
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %36) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %33) #18
  %110 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 0, i32 0
  %111 = load i8*, i8** %110, align 8, !tbaa !80
  %112 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 2
  %113 = bitcast %union.anon* %112 to i8*
  %114 = icmp eq i8* %111, %113
  br i1 %114, label %116, label %115

115:                                              ; preds = %109
  call void @_ZdlPv(i8* %111) #18
  br label %116

116:                                              ; preds = %115, %109
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %32) #18
  br label %144

117:                                              ; preds = %64, %62, %60
  %118 = landingpad { i8*, i32 }
          cleanup
  br label %126

119:                                              ; preds = %97
  %120 = landingpad { i8*, i32 }
          cleanup
  %121 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %122 = load i8*, i8** %121, align 8, !tbaa !80
  %123 = icmp eq i8* %122, %71
  br i1 %123, label %124, label %.sink.split

.sink.split:                                      ; preds = %119, %86
  %.sink = phi i8* [ %89, %86 ], [ %122, %119 ]
  %.ph = phi { i8*, i32 } [ %87, %86 ], [ %120, %119 ]
  call void @_ZdlPv(i8* %.sink) #18
  br label %124

124:                                              ; preds = %.sink.split, %119, %86
  %125 = phi { i8*, i32 } [ %87, %86 ], [ %120, %119 ], [ %.ph, %.sink.split ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %67) #18
  br label %126

126:                                              ; preds = %124, %117
  %127 = phi { i8*, i32 } [ %125, %124 ], [ %118, %117 ]
  %128 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 0
  br label %129

129:                                              ; preds = %126, %58
  %130 = phi %"class.std::ios_base"* [ %47, %58 ], [ %128, %126 ]
  %131 = phi { i8*, i32 } [ %59, %58 ], [ %127, %126 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %130) #18
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %44) #18
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  %132 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 0, i32 0
  %133 = load i8*, i8** %132, align 8, !tbaa !80
  %134 = icmp eq i8* %133, %43
  br i1 %134, label %136, label %135

135:                                              ; preds = %129
  call void @_ZdlPv(i8* %133) #18
  br label %136

136:                                              ; preds = %135, %129
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !58
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %36) #18
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %33) #18
  %137 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 0, i32 0
  %138 = load i8*, i8** %137, align 8, !tbaa !80
  %139 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 2
  %140 = bitcast %union.anon* %139 to i8*
  %141 = icmp eq i8* %138, %140
  br i1 %141, label %143, label %142

142:                                              ; preds = %136
  call void @_ZdlPv(i8* %138) #18
  br label %143

143:                                              ; preds = %142, %136
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %32) #18
  resume { i8*, i32 } %131

144:                                              ; preds = %116, %29, %25
  ret i1 %10
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind uwtable willreturn writeonly
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock17setNumberOfTuplesEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #17 align 2 {
  %3 = trunc i64 %1 to i32
  %4 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  store i32 %3, i32* %4, align 4, !tbaa !17
  ret void
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock12getWatermarkEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #1 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  %3 = load i64, i64* %2, align 8, !tbaa !106
  ret i64 %3
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind uwtable willreturn writeonly
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock12setWatermarkEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #17 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  store i64 %1, i64* %3, align 8, !tbaa !106
  ret void
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock17getSequenceNumberEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #1 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 3
  %3 = load i64, i64* %2, align 8, !tbaa !118
  ret i64 %3
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind uwtable willreturn writeonly
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock17setSequenceNumberEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #17 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 3
  store i64 %1, i64* %3, align 8, !tbaa !118
  ret void
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind uwtable willreturn writeonly
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock20setCreationTimestampEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #17 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  store i64 %1, i64* %3, align 8, !tbaa !104
  ret void
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock20getCreationTimestampEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #1 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  %3 = load i64, i64* %2, align 8, !tbaa !104
  ret i64 %3
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock11getOriginIdEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #1 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 5
  %3 = load i64, i64* %2, align 8, !tbaa !107
  ret i64 %3
}

; Function Attrs: mustprogress nofree norecurse nosync nounwind uwtable willreturn writeonly
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock11setOriginIdEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #17 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 5
  store i64 %1, i64* %3, align 8, !tbaa !107
  ret void
}

; Function Attrs: mustprogress uwtable
define dso_local void @_ZN3NES7Runtime6detail26zmqBufferRecyclingCallbackEPvS2_(i8* nocapture readnone %0, i8* %1) local_unnamed_addr #15 {
  %3 = bitcast i8* %1 to %"class.NES::Runtime::detail::BufferControlBlock"*
  %4 = tail call zeroext i1 @_ZN3NES7Runtime6detail18BufferControlBlock7releaseEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %3)
  ret void
}

attributes #0 = { alwaysinline mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { mustprogress nofree norecurse nosync nounwind readonly uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { mustprogress nofree nosync nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #3 = { argmemonly mustprogress nofree nounwind willreturn }
attributes #4 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { argmemonly mustprogress nofree nounwind willreturn writeonly }
attributes #6 = { argmemonly mustprogress nofree nosync nounwind willreturn }
attributes #7 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #8 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #9 = { inaccessiblememonly mustprogress nofree nosync nounwind willreturn }
attributes #10 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { mustprogress nofree norecurse nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #14 = { noinline noreturn nounwind }
attributes #15 = { mustprogress uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #16 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { mustprogress nofree norecurse nosync nounwind uwtable willreturn writeonly "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #18 = { nounwind }
attributes #19 = { noreturn }
attributes #20 = { builtin allocsize(0) }
attributes #21 = { noreturn nounwind }
attributes #22 = { builtin nounwind }

!llvm.dbg.cu = !{!0}
!llvm.module.flags = !{!2, !3}
!llvm.ident = !{!4, !4}

!0 = distinct !DICompileUnit(language: DW_LANG_C, file: !1, producer: "mlir", isOptimized: true, runtimeVersion: 0, emissionKind: FullDebug)
!1 = !DIFile(filename: "LLVMDialectModule", directory: "/")
!2 = !{i32 2, !"Debug Info Version", i32 3}
!3 = !{i32 1, !"wchar_size", i32 4}
!4 = !{!"Ubuntu clang version 12.0.0-3ubuntu1~20.04.5"}
!5 = distinct !DISubprogram(name: "execute", linkageName: "execute", scope: null, file: !6, line: 8, type: !7, scopeLine: 8, spFlags: DISPFlagDefinition | DISPFlagOptimized, unit: !0, retainedNodes: !8)
!6 = !DIFile(filename: "../../generatedMLIR/locationTest.mlir", directory: "/home/rudi/dima/mlir-approach/cmake-build-debug/tests")
!7 = !DISubroutineType(types: !8)
!8 = !{}
!9 = !DILocation(line: 17, column: 12, scope: !10)
!10 = !DILexicalBlockFile(scope: !5, file: !6, discriminator: 0)
!11 = !{!12, !13, i64 0}
!12 = !{!"_ZTSN3NES7Runtime11TupleBufferE", !13, i64 0, !13, i64 8, !16, i64 16}
!13 = !{!"any pointer", !14, i64 0}
!14 = !{!"omnipotent char", !15, i64 0}
!15 = !{!"Simple C++ TBAA"}
!16 = !{!"int", !14, i64 0}
!17 = !{!18, !16, i64 4}
!18 = !{!"_ZTSN3NES7Runtime6detail18BufferControlBlockE", !19, i64 0, !16, i64 4, !20, i64 8, !20, i64 16, !20, i64 24, !20, i64 32, !13, i64 40, !21, i64 48, !23, i64 56}
!19 = !{!"_ZTSSt6atomicIiE"}
!20 = !{!"long", !14, i64 0}
!21 = !{!"_ZTSSt6atomicIPN3NES7Runtime14BufferRecyclerEE", !22, i64 0}
!22 = !{!"_ZTSSt13__atomic_baseIPN3NES7Runtime14BufferRecyclerEE", !13, i64 0}
!23 = !{!"_ZTSSt8functionIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEE", !13, i64 24}
!24 = !DILocation(line: 18, column: 7, scope: !10)
!25 = !DILocation(line: 24, column: 13, scope: !10)
!26 = !DILocation(line: 25, column: 7, scope: !10)
!27 = !DILocation(line: 27, column: 13, scope: !10)
!28 = !DILocation(line: 31, column: 13, scope: !10)
!29 = !DILocation(line: 32, column: 7, scope: !10)
!30 = !DILocation(line: 34, column: 13, scope: !10)
!31 = !DILocation(line: 38, column: 13, scope: !10)
!32 = !DILocation(line: 39, column: 7, scope: !10)
!33 = !DILocation(line: 41, column: 13, scope: !10)
!34 = !DILocation(line: 45, column: 13, scope: !10)
!35 = !DILocation(line: 46, column: 7, scope: !10)
!36 = !DILocation(line: 48, column: 13, scope: !10)
!37 = !DILocation(line: 52, column: 13, scope: !10)
!38 = !DILocation(line: 53, column: 7, scope: !10)
!39 = !DILocation(line: 55, column: 13, scope: !10)
!40 = !DILocation(line: 59, column: 13, scope: !10)
!41 = !DILocation(line: 60, column: 7, scope: !10)
!42 = !DILocation(line: 62, column: 13, scope: !10)
!43 = !DILocation(line: 76, column: 5, scope: !10)
!44 = distinct !DISubprogram(name: "_mlir_ciface_execute", linkageName: "_mlir_ciface_execute", scope: null, file: !6, line: 8, type: !7, scopeLine: 8, spFlags: DISPFlagDefinition | DISPFlagOptimized, unit: !0, retainedNodes: !8)
!45 = !DILocation(line: 8, column: 3, scope: !46)
!46 = !DILexicalBlockFile(scope: !44, file: !6, discriminator: 0)
!47 = !{!48, !13, i64 0}
!48 = !{!"_ZTSN3NES7Runtime6detail13MemorySegmentE", !13, i64 0, !16, i64 8, !13, i64 16}
!49 = !{!48, !16, i64 8}
!50 = !{!48, !13, i64 16}
!51 = !{!18, !13, i64 40}
!52 = !{!22, !13, i64 0}
!53 = !{!54, !13, i64 16}
!54 = !{!"_ZTSSt14_Function_base", !14, i64 0, !13, i64 16}
!55 = !{i64 0, i64 8, !56, i64 0, i64 8, !56, i64 0, i64 8, !56, i64 0, i64 16, !56, i64 0, i64 16, !56}
!56 = !{!14, !14, i64 0}
!57 = !{!13, !13, i64 0}
!58 = !{!59, !59, i64 0}
!59 = !{!"vtable pointer", !15, i64 0}
!60 = !{!61, !62, i64 64}
!61 = !{!"_ZTSNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE", !62, i64 64, !63, i64 72}
!62 = !{!"_ZTSSt13_Ios_Openmode", !14, i64 0}
!63 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE", !64, i64 0, !20, i64 8, !14, i64 16}
!64 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE12_Alloc_hiderE", !13, i64 0}
!65 = !{!64, !13, i64 0}
!66 = !{!63, !20, i64 8}
!67 = !{!68, !13, i64 216}
!68 = !{!"_ZTSSt9basic_iosIcSt11char_traitsIcEE", !13, i64 216, !14, i64 224, !69, i64 225, !13, i64 232, !13, i64 240, !13, i64 248, !13, i64 256}
!69 = !{!"bool", !14, i64 0}
!70 = !{!68, !14, i64 224}
!71 = !{!68, !69, i64 225}
!72 = !{!73}
!73 = distinct !{!73, !74, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!74 = distinct !{!74, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!75 = !{!76, !13, i64 40}
!76 = !{!"_ZTSSt15basic_streambufIcSt11char_traitsIcEE", !13, i64 8, !13, i64 16, !13, i64 24, !13, i64 32, !13, i64 40, !13, i64 48, !77, i64 56}
!77 = !{!"_ZTSSt6locale", !13, i64 0}
!78 = !{!76, !13, i64 24}
!79 = !{!76, !13, i64 32}
!80 = !{!63, !13, i64 0}
!81 = !{!82, !13, i64 0}
!82 = !{!"_ZTSNSt12experimental15fundamentals_v215source_locationE", !13, i64 0, !13, i64 8, !16, i64 16, !16, i64 20}
!83 = !{!84}
!84 = distinct !{!84, !85, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii: argument 0"}
!85 = distinct !{!85, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii"}
!86 = !{!82, !13, i64 8}
!87 = !{!82, !16, i64 16}
!88 = !{!82, !16, i64 20}
!89 = !{!90}
!90 = distinct !{!90, !91, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!91 = distinct !{!91, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!92 = !{!93}
!93 = distinct !{!93, !94, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii: argument 0"}
!94 = distinct !{!94, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii"}
!95 = !{!96}
!96 = distinct !{!96, !97, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!97 = distinct !{!97, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!98 = !{!99}
!99 = distinct !{!99, !100, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!100 = distinct !{!100, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!101 = !{!102}
!102 = distinct !{!102, !103, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!103 = distinct !{!103, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!104 = !{!18, !20, i64 24}
!105 = !{!23, !13, i64 24}
!106 = !{!18, !20, i64 8}
!107 = !{!18, !20, i64 32}
!108 = !{!20, !20, i64 0}
!109 = !{!110}
!110 = distinct !{!110, !111, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!111 = distinct !{!111, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!112 = !{!113}
!113 = distinct !{!113, !114, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!114 = distinct !{!114, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!115 = !{!116}
!116 = distinct !{!116, !117, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!117 = distinct !{!117, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!118 = !{!18, !20, i64 16}