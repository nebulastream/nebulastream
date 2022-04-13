; ModuleID = 'llvm-link'
source_filename = "llvm-link"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.std::ios_base::Init" = type { i8 }
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

;@llvm.global_ctors = appending global [1 x { i32, void ()*, i8* }] [{ i32, void ()*, i8* } { i32 65535, void ()* @_GLOBAL__sub_I_TupleBufferImpl.cpp, i8* null }]
@_ZStL8__ioinit = internal global %"class.std::ios_base::Init" zeroinitializer, align 1
@__dso_handle = external hidden global i8
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

; Function Attrs: alwaysinline nounwind uwtable mustprogress
define dso_local i64 @getNumTuples(i8* nocapture readonly %0) local_unnamed_addr #0 {
  %2 = bitcast i8* %0 to %"class.NES::Runtime::detail::BufferControlBlock"**
  %3 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %2, align 8, !tbaa !2
  %4 = tail call i64 @_ZNK3NES7Runtime6detail18BufferControlBlock17getNumberOfTuplesEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %3) #19
  ret i64 %4
}

; Function Attrs: uwtable
define internal void @_GLOBAL__sub_I_TupleBufferImpl.cpp() #1 section ".text.startup" {
  tail call void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1) @_ZStL8__ioinit)
  %1 = tail call i32 @__cxa_atexit(void (i8*)* bitcast (void (%"class.std::ios_base::Init"*)* @_ZNSt8ios_base4InitD1Ev to void (i8*)*), i8* getelementptr inbounds (%"class.std::ios_base::Init", %"class.std::ios_base::Init"* @_ZStL8__ioinit, i64 0, i32 0), i8* nonnull @__dso_handle) #19
  ret void
}

declare dso_local void @_ZNSt8ios_base4InitC1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #2

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_base4InitD1Ev(%"class.std::ios_base::Init"* nonnull dereferenceable(1)) unnamed_addr #3

; Function Attrs: nofree nounwind
declare dso_local i32 @__cxa_atexit(void (i8*)*, i8*, i8*) local_unnamed_addr #4

; Function Attrs: nofree nounwind uwtable willreturn
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentC2ERKS2_(%"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull dereferenceable(24) %0, %"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull readonly align 8 dereferenceable(24) %1) unnamed_addr #5 align 2 {
  %3 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %0 to i8*
  %4 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %1 to i8*
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %3, i8* nonnull align 8 dereferenceable(24) %4, i64 24, i1 false)
  ret void
}

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.memcpy.p0i8.p0i8.i64(i8* noalias nocapture writeonly, i8* noalias nocapture readonly, i64, i1 immarg) #6

; Function Attrs: nofree nounwind uwtable willreturn
define dso_local nonnull align 8 dereferenceable(24) %"class.NES::Runtime::detail::MemorySegment"* @_ZN3NES7Runtime6detail13MemorySegmentaSERKS2_(%"class.NES::Runtime::detail::MemorySegment"* nonnull returned dereferenceable(24) %0, %"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull readonly align 8 dereferenceable(24) %1) local_unnamed_addr #5 align 2 {
  %3 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %0 to i8*
  %4 = bitcast %"class.NES::Runtime::detail::MemorySegment"* %1 to i8*
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(24) %3, i8* nonnull align 8 dereferenceable(24) %4, i64 24, i1 false)
  ret %"class.NES::Runtime::detail::MemorySegment"* %0
}

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentC2EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EE(%"class.NES::Runtime::detail::MemorySegment"* nonnull dereferenceable(24) %0, i8* %1, i32 %2, %"class.NES::Runtime::BufferRecycler"* %3, %"class.std::function"* nocapture nonnull align 8 dereferenceable(32) %4) unnamed_addr #1 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
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
  store i8* %1, i8** %17, align 8, !tbaa !8
  %18 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 1
  store i32 %2, i32* %18, align 8, !tbaa !10
  %19 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 2
  store %"class.NES::Runtime::detail::BufferControlBlock"* null, %"class.NES::Runtime::detail::BufferControlBlock"** %19, align 8, !tbaa !11
  %20 = zext i32 %2 to i64
  %21 = getelementptr inbounds i8, i8* %1, i64 %20
  %22 = getelementptr inbounds i8, i8* %21, i64 40
  %23 = bitcast i8* %22 to %"class.NES::Runtime::detail::MemorySegment"**
  tail call void @llvm.memset.p0i8.i64(i8* nonnull align 4 dereferenceable(40) %21, i8 0, i64 40, i1 false) #19
  store %"class.NES::Runtime::detail::MemorySegment"* %0, %"class.NES::Runtime::detail::MemorySegment"** %23, align 8, !tbaa !12
  %24 = getelementptr inbounds i8, i8* %21, i64 48
  %25 = bitcast i8* %24 to %"class.NES::Runtime::BufferRecycler"**
  store %"class.NES::Runtime::BufferRecycler"* %3, %"class.NES::Runtime::BufferRecycler"** %25, align 8, !tbaa !19
  %26 = getelementptr inbounds i8, i8* %21, i64 56
  %27 = getelementptr inbounds i8, i8* %21, i64 72
  %28 = bitcast i8* %27 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %28, align 8, !tbaa !20
  %29 = bitcast { i64, i64 }* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %29)
  %30 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %29, i8* nonnull align 8 dereferenceable(16) %30, i64 16, i1 false) #19, !tbaa.struct !22
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %30, i8* nonnull align 8 dereferenceable(16) %26, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %26, i8* nonnull align 8 dereferenceable(16) %29, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %29)
  %31 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  %32 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %31, align 8, !tbaa !24
  %33 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %28, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %33, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %31, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %32, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %28, align 8, !tbaa !24
  %34 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %35 = getelementptr inbounds i8, i8* %21, i64 80
  %36 = bitcast i8* %35 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  %37 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %34, align 8, !tbaa !24
  %38 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %36, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %38, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %34, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %37, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %36, align 8, !tbaa !24
  %39 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"** %19 to i8**
  store i8* %21, i8** %39, align 8, !tbaa !11
  %40 = load i8*, i8** %17, align 8, !tbaa !8
  %41 = icmp eq i8* %40, null
  br i1 %41, label %42, label %142

42:                                               ; preds = %5
  %43 = bitcast %"class.std::__cxx11::basic_string"* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %43) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %7)
  %44 = bitcast %"class.std::__cxx11::basic_stringbuf"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %44) #19
  %45 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !25
  %46 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 1
  %47 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 7
  %48 = bitcast i8** %46 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %48, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %47) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !25
  %49 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 1
  store i32 24, i32* %49, align 8, !tbaa !27
  %50 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2
  %51 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 2
  %52 = bitcast %"class.std::__cxx11::basic_string"* %50 to %union.anon**
  store %union.anon* %51, %union.anon** %52, align 8, !tbaa !32
  %53 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 1
  store i64 0, i64* %53, align 8, !tbaa !33
  %54 = bitcast %union.anon* %51 to i8*
  store i8 0, i8* %54, align 8, !tbaa !23
  %55 = bitcast %"class.std::basic_ostream"* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %55) #19
  %56 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0
  %57 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1
  %58 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %57, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %58) #19
  %59 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %57, i64 0, i32 0, i32 0
  %60 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %60, align 8, !tbaa !34
  %61 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 2
  store i8 0, i8* %61, align 8, !tbaa !37
  %62 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 3
  store i8 0, i8* %62, align 1, !tbaa !38
  %63 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 4
  %64 = bitcast %"class.std::basic_streambuf"** %63 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %64, i8 0, i64 32, i1 false) #19
  %65 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %65, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %59, align 8, !tbaa !25
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
  %74 = call i8* @__cxa_allocate_exception(i64 40) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !39)
  %75 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 2
  %76 = bitcast %"class.std::__cxx11::basic_string"* %10 to %union.anon**
  store %union.anon* %75, %union.anon** %76, align 8, !tbaa !32, !alias.scope !39
  %77 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 1
  store i64 0, i64* %77, align 8, !tbaa !33, !alias.scope !39
  %78 = bitcast %union.anon* %75 to i8*
  store i8 0, i8* %78, align 8, !tbaa !23, !alias.scope !39
  %79 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 5
  %80 = load i8*, i8** %79, align 8, !tbaa !42, !noalias !39
  %81 = icmp eq i8* %80, null
  br i1 %81, label %104, label %82

82:                                               ; preds = %73
  %83 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 3
  %84 = load i8*, i8** %83, align 8, !tbaa !45, !noalias !39
  %85 = icmp ugt i8* %80, %84
  %86 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 4
  %87 = load i8*, i8** %86, align 8, !tbaa !46
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
  %96 = load i8*, i8** %95, align 8, !tbaa !47, !alias.scope !39
  %97 = icmp eq i8* %96, %78
  br i1 %97, label %122, label %98

98:                                               ; preds = %93
  call void @_ZdlPv(i8* %96) #19
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
  store i8* getelementptr inbounds ([20 x i8], [20 x i8]* @.str.1, i64 0, i64 0), i8** %107, align 8, !tbaa !48, !alias.scope !50
  store i8* getelementptr inbounds ([14 x i8], [14 x i8]* @.str.2, i64 0, i64 0), i8** %108, align 8, !tbaa !53, !alias.scope !50
  store i32 45, i32* %109, align 8, !tbaa !54, !alias.scope !50
  store i32 0, i32* %110, align 4, !tbaa !55, !alias.scope !50
  invoke void @_ZN3NES10Exceptions16RuntimeExceptionC1ENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS7_NSt12experimental15fundamentals_v215source_locationE(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %106, %"class.std::__cxx11::basic_string"* nonnull %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %7, %"struct.std::experimental::fundamentals_v2::source_location"* nonnull byval(%"struct.std::experimental::fundamentals_v2::source_location") align 8 %11)
          to label %111 unwind label %114

111:                                              ; preds = %105
  invoke void @__cxa_throw(i8* %74, i8* bitcast (i8** @_ZTIN3NES10Exceptions16RuntimeExceptionE to i8*), i8* bitcast (void (%"class.NES::Exceptions::RuntimeException"*)* @_ZN3NES10Exceptions16RuntimeExceptionD1Ev to i8*)) #20
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
  %118 = load i8*, i8** %117, align 8, !tbaa !47
  %119 = icmp eq i8* %118, %78
  br i1 %119, label %121, label %120

120:                                              ; preds = %114
  call void @_ZdlPv(i8* %118) #19
  br i1 %115, label %122, label %124

121:                                              ; preds = %114
  br i1 %115, label %122, label %124

122:                                              ; preds = %121, %120, %98, %93
  %123 = phi { i8*, i32 } [ %116, %121 ], [ %116, %120 ], [ %94, %93 ], [ %94, %98 ]
  call void @__cxa_free_exception(i8* %74) #19
  br label %124

124:                                              ; preds = %122, %121, %120, %112
  %125 = phi { i8*, i32 } [ %123, %122 ], [ %116, %121 ], [ %113, %112 ], [ %116, %120 ]
  %126 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 0
  br label %127

127:                                              ; preds = %124, %69
  %128 = phi %"class.std::ios_base"* [ %58, %69 ], [ %126, %124 ]
  %129 = phi { i8*, i32 } [ %70, %69 ], [ %125, %124 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %128) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %55) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !25
  %130 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 0, i32 0
  %131 = load i8*, i8** %130, align 8, !tbaa !47
  %132 = icmp eq i8* %131, %54
  br i1 %132, label %134, label %133

133:                                              ; preds = %127
  call void @_ZdlPv(i8* %131) #19
  br label %134

134:                                              ; preds = %133, %127
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %45, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %47) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %44) #19
  %135 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %136 = load i8*, i8** %135, align 8, !tbaa !47
  %137 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %138 = bitcast %union.anon* %137 to i8*
  %139 = icmp eq i8* %136, %138
  br i1 %139, label %141, label %140

140:                                              ; preds = %134
  call void @_ZdlPv(i8* %136) #19
  br label %141

141:                                              ; preds = %140, %134
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %43) #19
  br label %246

142:                                              ; preds = %5
  %143 = load i32, i32* %18, align 8, !tbaa !10
  %144 = icmp eq i32 %143, 0
  br i1 %144, label %145, label %245

145:                                              ; preds = %142
  %146 = bitcast %"class.std::__cxx11::basic_string"* %12 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %146) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %12)
  %147 = bitcast %"class.std::__cxx11::basic_stringbuf"* %13 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %147) #19
  %148 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !25
  %149 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 1
  %150 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 7
  %151 = bitcast i8** %149 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %151, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %150) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !25
  %152 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 1
  store i32 24, i32* %152, align 8, !tbaa !27
  %153 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2
  %154 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 2
  %155 = bitcast %"class.std::__cxx11::basic_string"* %153 to %union.anon**
  store %union.anon* %154, %union.anon** %155, align 8, !tbaa !32
  %156 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 1
  store i64 0, i64* %156, align 8, !tbaa !33
  %157 = bitcast %union.anon* %154 to i8*
  store i8 0, i8* %157, align 8, !tbaa !23
  %158 = bitcast %"class.std::basic_ostream"* %14 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %158) #19
  %159 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0
  %160 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1
  %161 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %160, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %161) #19
  %162 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %160, i64 0, i32 0, i32 0
  %163 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %163, align 8, !tbaa !34
  %164 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 2
  store i8 0, i8* %164, align 8, !tbaa !37
  %165 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 3
  store i8 0, i8* %165, align 1, !tbaa !38
  %166 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 4
  %167 = bitcast %"class.std::basic_streambuf"** %166 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %167, i8 0, i64 32, i1 false) #19
  %168 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %168, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %162, align 8, !tbaa !25
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
  %177 = call i8* @__cxa_allocate_exception(i64 40) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !56)
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 2
  %179 = bitcast %"class.std::__cxx11::basic_string"* %15 to %union.anon**
  store %union.anon* %178, %union.anon** %179, align 8, !tbaa !32, !alias.scope !56
  %180 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 1
  store i64 0, i64* %180, align 8, !tbaa !33, !alias.scope !56
  %181 = bitcast %union.anon* %178 to i8*
  store i8 0, i8* %181, align 8, !tbaa !23, !alias.scope !56
  %182 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 5
  %183 = load i8*, i8** %182, align 8, !tbaa !42, !noalias !56
  %184 = icmp eq i8* %183, null
  br i1 %184, label %207, label %185

185:                                              ; preds = %176
  %186 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 3
  %187 = load i8*, i8** %186, align 8, !tbaa !45, !noalias !56
  %188 = icmp ugt i8* %183, %187
  %189 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 4
  %190 = load i8*, i8** %189, align 8, !tbaa !46
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
  %199 = load i8*, i8** %198, align 8, !tbaa !47, !alias.scope !56
  %200 = icmp eq i8* %199, %181
  br i1 %200, label %225, label %201

201:                                              ; preds = %196
  call void @_ZdlPv(i8* %199) #19
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
  store i8* getelementptr inbounds ([20 x i8], [20 x i8]* @.str.1, i64 0, i64 0), i8** %210, align 8, !tbaa !48, !alias.scope !59
  store i8* getelementptr inbounds ([14 x i8], [14 x i8]* @.str.2, i64 0, i64 0), i8** %211, align 8, !tbaa !53, !alias.scope !59
  store i32 48, i32* %212, align 8, !tbaa !54, !alias.scope !59
  store i32 0, i32* %213, align 4, !tbaa !55, !alias.scope !59
  invoke void @_ZN3NES10Exceptions16RuntimeExceptionC1ENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS7_NSt12experimental15fundamentals_v215source_locationE(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %209, %"class.std::__cxx11::basic_string"* nonnull %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %12, %"struct.std::experimental::fundamentals_v2::source_location"* nonnull byval(%"struct.std::experimental::fundamentals_v2::source_location") align 8 %16)
          to label %214 unwind label %217

214:                                              ; preds = %208
  invoke void @__cxa_throw(i8* %177, i8* bitcast (i8** @_ZTIN3NES10Exceptions16RuntimeExceptionE to i8*), i8* bitcast (void (%"class.NES::Exceptions::RuntimeException"*)* @_ZN3NES10Exceptions16RuntimeExceptionD1Ev to i8*)) #20
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
  %221 = load i8*, i8** %220, align 8, !tbaa !47
  %222 = icmp eq i8* %221, %181
  br i1 %222, label %224, label %223

223:                                              ; preds = %217
  call void @_ZdlPv(i8* %221) #19
  br i1 %218, label %225, label %227

224:                                              ; preds = %217
  br i1 %218, label %225, label %227

225:                                              ; preds = %224, %223, %201, %196
  %226 = phi { i8*, i32 } [ %219, %224 ], [ %219, %223 ], [ %197, %196 ], [ %197, %201 ]
  call void @__cxa_free_exception(i8* %177) #19
  br label %227

227:                                              ; preds = %225, %224, %223, %215
  %228 = phi { i8*, i32 } [ %226, %225 ], [ %219, %224 ], [ %216, %215 ], [ %219, %223 ]
  %229 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 0
  br label %230

230:                                              ; preds = %227, %172
  %231 = phi %"class.std::ios_base"* [ %161, %172 ], [ %229, %227 ]
  %232 = phi { i8*, i32 } [ %173, %172 ], [ %228, %227 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %231) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %158) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !25
  %233 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 0, i32 0
  %234 = load i8*, i8** %233, align 8, !tbaa !47
  %235 = icmp eq i8* %234, %157
  br i1 %235, label %237, label %236

236:                                              ; preds = %230
  call void @_ZdlPv(i8* %234) #19
  br label %237

237:                                              ; preds = %236, %230
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %148, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %150) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %147) #19
  %238 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 0, i32 0
  %239 = load i8*, i8** %238, align 8, !tbaa !47
  %240 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 2
  %241 = bitcast %union.anon* %240 to i8*
  %242 = icmp eq i8* %239, %241
  br i1 %242, label %244, label %243

243:                                              ; preds = %237
  call void @_ZdlPv(i8* %239) #19
  br label %244

244:                                              ; preds = %243, %237
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %146) #19
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

; Function Attrs: argmemonly nofree nosync nounwind willreturn writeonly
declare void @llvm.memset.p0i8.i64(i8* nocapture writeonly, i8, i64, i1 immarg) #7

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.start.p0i8(i64 immarg, i8* nocapture) #6

; Function Attrs: argmemonly nofree nosync nounwind willreturn
declare void @llvm.lifetime.end.p0i8(i64 immarg, i8* nocapture) #6

declare dso_local void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* sret(%"class.std::__cxx11::basic_string") align 8) local_unnamed_addr #2

; Function Attrs: nounwind
declare dso_local void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8)) unnamed_addr #3

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216)) unnamed_addr #3

declare dso_local void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264), %"class.std::basic_streambuf"*) local_unnamed_addr #2

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8), i8*, i64) local_unnamed_addr #2

declare dso_local i8* @__cxa_allocate_exception(i64) local_unnamed_addr

; Function Attrs: inaccessiblememonly nofree nosync nounwind willreturn
declare void @llvm.experimental.noalias.scope.decl(metadata) #8

declare dso_local nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32), i64, i64, i8*, i64) local_unnamed_addr #2

; Function Attrs: nobuiltin nounwind
declare dso_local void @_ZdlPv(i8*) local_unnamed_addr #9

declare dso_local void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32), %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32)) local_unnamed_addr #2

declare dso_local void @_ZN3NES10Exceptions16RuntimeExceptionC1ENSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS7_NSt12experimental15fundamentals_v215source_locationE(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40), %"class.std::__cxx11::basic_string"*, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32), %"struct.std::experimental::fundamentals_v2::source_location"* byval(%"struct.std::experimental::fundamentals_v2::source_location") align 8) unnamed_addr #2

; Function Attrs: nounwind uwtable
define linkonce_odr dso_local void @_ZN3NES10Exceptions16RuntimeExceptionD1Ev(%"class.NES::Exceptions::RuntimeException"* nonnull dereferenceable(40) %0) unnamed_addr #10 comdat align 2 personality i32 (...)* @__gxx_personality_v0 {
  %2 = load i32 (...)**, i32 (...)*** bitcast ([2 x i8*]* @_ZTTN3NES10Exceptions16RuntimeExceptionE to i32 (...)***), align 8
  %3 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 0, i32 0
  store i32 (...)** %2, i32 (...)*** %3, align 8, !tbaa !25
  %4 = load i32 (...)**, i32 (...)*** bitcast (i8** getelementptr inbounds ([2 x i8*], [2 x i8*]* @_ZTTN3NES10Exceptions16RuntimeExceptionE, i64 0, i64 1) to i32 (...)***), align 8
  %5 = getelementptr i32 (...)*, i32 (...)** %2, i64 -5
  %6 = bitcast i32 (...)** %5 to i64*
  %7 = load i64, i64* %6, align 8
  %8 = bitcast %"class.NES::Exceptions::RuntimeException"* %0 to i8*
  %9 = getelementptr inbounds i8, i8* %8, i64 %7
  %10 = bitcast i8* %9 to i32 (...)***
  store i32 (...)** %4, i32 (...)*** %10, align 8, !tbaa !25
  %11 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 1, i32 0, i32 0
  %12 = load i8*, i8** %11, align 8, !tbaa !47
  %13 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 1, i32 2
  %14 = bitcast %union.anon* %13 to i8*
  %15 = icmp eq i8* %12, %14
  br i1 %15, label %17, label %16

16:                                               ; preds = %1
  tail call void @_ZdlPv(i8* %12) #19
  br label %17

17:                                               ; preds = %16, %1
  %18 = getelementptr inbounds %"class.NES::Exceptions::RuntimeException", %"class.NES::Exceptions::RuntimeException"* %0, i64 0, i32 0
  tail call void @_ZNSt9exceptionD2Ev(%"class.std::exception"* nonnull dereferenceable(8) %18) #19
  ret void
}

declare dso_local void @__cxa_throw(i8*, i8*, i8*) local_unnamed_addr

declare dso_local void @__cxa_free_exception(i8*) local_unnamed_addr

; Function Attrs: nounwind
declare dso_local void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216)) unnamed_addr #3

; Function Attrs: nounwind
declare dso_local void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8)) unnamed_addr #3

; Function Attrs: nounwind
declare dso_local void @_ZNSt9exceptionD2Ev(%"class.std::exception"* nonnull dereferenceable(8)) unnamed_addr #3

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentC2EPhjPNS0_14BufferRecyclerEOSt8functionIFvPS2_S5_EEb(%"class.NES::Runtime::detail::MemorySegment"* nonnull dereferenceable(24) %0, i8* %1, i32 %2, %"class.NES::Runtime::BufferRecycler"* %3, %"class.std::function"* nocapture nonnull align 8 dereferenceable(32) %4, i1 zeroext %5) unnamed_addr #1 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
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
  store i8* %1, i8** %16, align 8, !tbaa !8
  %17 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 1
  store i32 %2, i32* %17, align 8, !tbaa !10
  %18 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 2
  store %"class.NES::Runtime::detail::BufferControlBlock"* null, %"class.NES::Runtime::detail::BufferControlBlock"** %18, align 8, !tbaa !11
  %19 = icmp eq i8* %1, null
  br i1 %19, label %20, label %136

20:                                               ; preds = %6
  %21 = bitcast %"class.std::__cxx11::basic_string"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %21) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %8)
  %22 = bitcast %"class.std::__cxx11::basic_stringbuf"* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %22) #19
  %23 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !25
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 1
  %25 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 7
  %26 = bitcast i8** %24 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %26, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %25) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !25
  %27 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 1
  store i32 24, i32* %27, align 8, !tbaa !27
  %28 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2
  %29 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 2
  %30 = bitcast %"class.std::__cxx11::basic_string"* %28 to %union.anon**
  store %union.anon* %29, %union.anon** %30, align 8, !tbaa !32
  %31 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 1
  store i64 0, i64* %31, align 8, !tbaa !33
  %32 = bitcast %union.anon* %29 to i8*
  store i8 0, i8* %32, align 8, !tbaa !23
  %33 = bitcast %"class.std::basic_ostream"* %10 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %33) #19
  %34 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0
  %35 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1
  %36 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %35, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %36) #19
  %37 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %35, i64 0, i32 0, i32 0
  %38 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %38, align 8, !tbaa !34
  %39 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 2
  store i8 0, i8* %39, align 8, !tbaa !37
  %40 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 3
  store i8 0, i8* %40, align 1, !tbaa !38
  %41 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 4
  %42 = bitcast %"class.std::basic_streambuf"** %41 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %42, i8 0, i64 32, i1 false) #19
  %43 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %43, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %37, align 8, !tbaa !25
  %44 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %45 = getelementptr inbounds i8, i8* %33, i64 %44
  %46 = bitcast i8* %45 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %46, %"class.std::basic_streambuf"* nonnull %34)
          to label %49 unwind label %47

47:                                               ; preds = %20
  %48 = landingpad { i8*, i32 }
          cleanup
  br label %121

49:                                               ; preds = %20
  %50 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %10, i8* nonnull getelementptr inbounds ([30 x i8], [30 x i8]* @.str.4, i64 0, i64 0), i64 29)
          to label %51 unwind label %108

51:                                               ; preds = %49
  %52 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %10, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %53 unwind label %108

53:                                               ; preds = %51
  %54 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %10, i8* nonnull getelementptr inbounds ([12 x i8], [12 x i8]* @.str.6, i64 0, i64 0), i64 11)
          to label %55 unwind label %108

55:                                               ; preds = %53
  %56 = bitcast %"class.std::__cxx11::basic_string"* %11 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %56) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !62)
  %57 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 2
  %58 = bitcast %"class.std::__cxx11::basic_string"* %11 to %union.anon**
  store %union.anon* %57, %union.anon** %58, align 8, !tbaa !32, !alias.scope !62
  %59 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 1
  store i64 0, i64* %59, align 8, !tbaa !33, !alias.scope !62
  %60 = bitcast %union.anon* %57 to i8*
  store i8 0, i8* %60, align 8, !tbaa !23, !alias.scope !62
  %61 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 5
  %62 = load i8*, i8** %61, align 8, !tbaa !42, !noalias !62
  %63 = icmp eq i8* %62, null
  br i1 %63, label %86, label %64

64:                                               ; preds = %55
  %65 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 3
  %66 = load i8*, i8** %65, align 8, !tbaa !45, !noalias !62
  %67 = icmp ugt i8* %62, %66
  %68 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 0, i32 4
  %69 = load i8*, i8** %68, align 8, !tbaa !46
  br i1 %67, label %70, label %81

70:                                               ; preds = %64
  %71 = ptrtoint i8* %62 to i64
  %72 = ptrtoint i8* %69 to i64
  %73 = sub i64 %71, %72
  %74 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %11, i64 0, i64 0, i8* %69, i64 %73)
          to label %87 unwind label %75

75:                                               ; preds = %86, %81, %70
  %76 = landingpad { i8*, i32 }
          cleanup
  %77 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 0, i32 0
  %78 = load i8*, i8** %77, align 8, !tbaa !47, !alias.scope !62
  %79 = icmp eq i8* %78, %60
  br i1 %79, label %116, label %80

80:                                               ; preds = %75
  call void @_ZdlPv(i8* %78) #19
  br label %116

81:                                               ; preds = %64
  %82 = ptrtoint i8* %66 to i64
  %83 = ptrtoint i8* %69 to i64
  %84 = sub i64 %82, %83
  %85 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %11, i64 0, i64 0, i8* %69, i64 %84)
          to label %87 unwind label %75

86:                                               ; preds = %55
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %11, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %28)
          to label %87 unwind label %75

87:                                               ; preds = %86, %81, %70
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %11, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %8)
          to label %88 unwind label %110

88:                                               ; preds = %87
  %89 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 0, i32 0
  %90 = load i8*, i8** %89, align 8, !tbaa !47
  %91 = icmp eq i8* %90, %60
  br i1 %91, label %93, label %92

92:                                               ; preds = %88
  call void @_ZdlPv(i8* %90) #19
  br label %93

93:                                               ; preds = %92, %88
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #19
  %94 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %94) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %33) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !25
  %95 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 0, i32 0
  %96 = load i8*, i8** %95, align 8, !tbaa !47
  %97 = icmp eq i8* %96, %32
  br i1 %97, label %99, label %98

98:                                               ; preds = %93
  call void @_ZdlPv(i8* %96) #19
  br label %99

99:                                               ; preds = %98, %93
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %25) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %22) #19
  %100 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 0, i32 0
  %101 = load i8*, i8** %100, align 8, !tbaa !47
  %102 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 2
  %103 = bitcast %union.anon* %102 to i8*
  %104 = icmp eq i8* %101, %103
  br i1 %104, label %106, label %105

105:                                              ; preds = %99
  call void @_ZdlPv(i8* %101) #19
  br label %106

106:                                              ; preds = %105, %99
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %21) #19
  %107 = load i32, i32* %17, align 8, !tbaa !10
  br label %136

108:                                              ; preds = %53, %51, %49
  %109 = landingpad { i8*, i32 }
          cleanup
  br label %118

110:                                              ; preds = %87
  %111 = landingpad { i8*, i32 }
          cleanup
  %112 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %11, i64 0, i32 0, i32 0
  %113 = load i8*, i8** %112, align 8, !tbaa !47
  %114 = icmp eq i8* %113, %60
  br i1 %114, label %116, label %115

115:                                              ; preds = %110
  call void @_ZdlPv(i8* %113) #19
  br label %116

116:                                              ; preds = %115, %110, %80, %75
  %117 = phi { i8*, i32 } [ %76, %80 ], [ %76, %75 ], [ %111, %110 ], [ %111, %115 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #19
  br label %118

118:                                              ; preds = %116, %108
  %119 = phi { i8*, i32 } [ %117, %116 ], [ %109, %108 ]
  %120 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %10, i64 0, i32 1, i32 0
  br label %121

121:                                              ; preds = %118, %47
  %122 = phi %"class.std::ios_base"* [ %36, %47 ], [ %120, %118 ]
  %123 = phi { i8*, i32 } [ %48, %47 ], [ %119, %118 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %122) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %33) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !25
  %124 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %9, i64 0, i32 2, i32 0, i32 0
  %125 = load i8*, i8** %124, align 8, !tbaa !47
  %126 = icmp eq i8* %125, %32
  br i1 %126, label %128, label %127

127:                                              ; preds = %121
  call void @_ZdlPv(i8* %125) #19
  br label %128

128:                                              ; preds = %127, %121
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %23, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %25) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %22) #19
  %129 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 0, i32 0
  %130 = load i8*, i8** %129, align 8, !tbaa !47
  %131 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %8, i64 0, i32 2
  %132 = bitcast %union.anon* %131 to i8*
  %133 = icmp eq i8* %130, %132
  br i1 %133, label %135, label %134

134:                                              ; preds = %128
  call void @_ZdlPv(i8* %130) #19
  br label %135

135:                                              ; preds = %134, %128
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %21) #19
  br label %274

136:                                              ; preds = %106, %6
  %137 = phi i32 [ %107, %106 ], [ %2, %6 ]
  %138 = icmp eq i32 %137, 0
  br i1 %138, label %139, label %254

139:                                              ; preds = %136
  %140 = bitcast %"class.std::__cxx11::basic_string"* %12 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %140) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %12)
  %141 = bitcast %"class.std::__cxx11::basic_stringbuf"* %13 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %141) #19
  %142 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %143 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 1
  %144 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 7
  %145 = bitcast i8** %143 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %145, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %144) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %146 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 1
  store i32 24, i32* %146, align 8, !tbaa !27
  %147 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2
  %148 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 2
  %149 = bitcast %"class.std::__cxx11::basic_string"* %147 to %union.anon**
  store %union.anon* %148, %union.anon** %149, align 8, !tbaa !32
  %150 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 1
  store i64 0, i64* %150, align 8, !tbaa !33
  %151 = bitcast %union.anon* %148 to i8*
  store i8 0, i8* %151, align 8, !tbaa !23
  %152 = bitcast %"class.std::basic_ostream"* %14 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %152) #19
  %153 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0
  %154 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1
  %155 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %154, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %155) #19
  %156 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %154, i64 0, i32 0, i32 0
  %157 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %157, align 8, !tbaa !34
  %158 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 2
  store i8 0, i8* %158, align 8, !tbaa !37
  %159 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 3
  store i8 0, i8* %159, align 1, !tbaa !38
  %160 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 4
  %161 = bitcast %"class.std::basic_streambuf"** %160 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %161, i8 0, i64 32, i1 false) #19
  %162 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %162, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %156, align 8, !tbaa !25
  %163 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %164 = getelementptr inbounds i8, i8* %152, i64 %163
  %165 = bitcast i8* %164 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %165, %"class.std::basic_streambuf"* nonnull %153)
          to label %168 unwind label %166

166:                                              ; preds = %139
  %167 = landingpad { i8*, i32 }
          cleanup
  br label %239

168:                                              ; preds = %139
  %169 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([31 x i8], [31 x i8]* @.str.7, i64 0, i64 0), i64 30)
          to label %170 unwind label %226

170:                                              ; preds = %168
  %171 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %172 unwind label %226

172:                                              ; preds = %170
  %173 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %14, i8* nonnull getelementptr inbounds ([13 x i8], [13 x i8]* @.str.8, i64 0, i64 0), i64 12)
          to label %174 unwind label %226

174:                                              ; preds = %172
  %175 = bitcast %"class.std::__cxx11::basic_string"* %15 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %175) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !65)
  %176 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 2
  %177 = bitcast %"class.std::__cxx11::basic_string"* %15 to %union.anon**
  store %union.anon* %176, %union.anon** %177, align 8, !tbaa !32, !alias.scope !65
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 1
  store i64 0, i64* %178, align 8, !tbaa !33, !alias.scope !65
  %179 = bitcast %union.anon* %176 to i8*
  store i8 0, i8* %179, align 8, !tbaa !23, !alias.scope !65
  %180 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 5
  %181 = load i8*, i8** %180, align 8, !tbaa !42, !noalias !65
  %182 = icmp eq i8* %181, null
  br i1 %182, label %205, label %183

183:                                              ; preds = %174
  %184 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 3
  %185 = load i8*, i8** %184, align 8, !tbaa !45, !noalias !65
  %186 = icmp ugt i8* %181, %185
  %187 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 0, i32 4
  %188 = load i8*, i8** %187, align 8, !tbaa !46
  br i1 %186, label %189, label %200

189:                                              ; preds = %183
  %190 = ptrtoint i8* %181 to i64
  %191 = ptrtoint i8* %188 to i64
  %192 = sub i64 %190, %191
  %193 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, i64 0, i64 0, i8* %188, i64 %192)
          to label %206 unwind label %194

194:                                              ; preds = %205, %200, %189
  %195 = landingpad { i8*, i32 }
          cleanup
  %196 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %197 = load i8*, i8** %196, align 8, !tbaa !47, !alias.scope !65
  %198 = icmp eq i8* %197, %179
  br i1 %198, label %234, label %199

199:                                              ; preds = %194
  call void @_ZdlPv(i8* %197) #19
  br label %234

200:                                              ; preds = %183
  %201 = ptrtoint i8* %185 to i64
  %202 = ptrtoint i8* %188 to i64
  %203 = sub i64 %201, %202
  %204 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, i64 0, i64 0, i8* %188, i64 %203)
          to label %206 unwind label %194

205:                                              ; preds = %174
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %147)
          to label %206 unwind label %194

206:                                              ; preds = %205, %200, %189
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %15, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %12)
          to label %207 unwind label %228

207:                                              ; preds = %206
  %208 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %209 = load i8*, i8** %208, align 8, !tbaa !47
  %210 = icmp eq i8* %209, %179
  br i1 %210, label %212, label %211

211:                                              ; preds = %207
  call void @_ZdlPv(i8* %209) #19
  br label %212

212:                                              ; preds = %211, %207
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %175) #19
  %213 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %213) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %152) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %214 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 0, i32 0
  %215 = load i8*, i8** %214, align 8, !tbaa !47
  %216 = icmp eq i8* %215, %151
  br i1 %216, label %218, label %217

217:                                              ; preds = %212
  call void @_ZdlPv(i8* %215) #19
  br label %218

218:                                              ; preds = %217, %212
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %144) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %141) #19
  %219 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 0, i32 0
  %220 = load i8*, i8** %219, align 8, !tbaa !47
  %221 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 2
  %222 = bitcast %union.anon* %221 to i8*
  %223 = icmp eq i8* %220, %222
  br i1 %223, label %225, label %224

224:                                              ; preds = %218
  call void @_ZdlPv(i8* %220) #19
  br label %225

225:                                              ; preds = %224, %218
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %140) #19
  br label %254

226:                                              ; preds = %172, %170, %168
  %227 = landingpad { i8*, i32 }
          cleanup
  br label %236

228:                                              ; preds = %206
  %229 = landingpad { i8*, i32 }
          cleanup
  %230 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %15, i64 0, i32 0, i32 0
  %231 = load i8*, i8** %230, align 8, !tbaa !47
  %232 = icmp eq i8* %231, %179
  br i1 %232, label %234, label %233

233:                                              ; preds = %228
  call void @_ZdlPv(i8* %231) #19
  br label %234

234:                                              ; preds = %233, %228, %199, %194
  %235 = phi { i8*, i32 } [ %195, %199 ], [ %195, %194 ], [ %229, %228 ], [ %229, %233 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %175) #19
  br label %236

236:                                              ; preds = %234, %226
  %237 = phi { i8*, i32 } [ %235, %234 ], [ %227, %226 ]
  %238 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %14, i64 0, i32 1, i32 0
  br label %239

239:                                              ; preds = %236, %166
  %240 = phi %"class.std::ios_base"* [ %155, %166 ], [ %238, %236 ]
  %241 = phi { i8*, i32 } [ %167, %166 ], [ %237, %236 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %240) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %152) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %242 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %13, i64 0, i32 2, i32 0, i32 0
  %243 = load i8*, i8** %242, align 8, !tbaa !47
  %244 = icmp eq i8* %243, %151
  br i1 %244, label %246, label %245

245:                                              ; preds = %239
  call void @_ZdlPv(i8* %243) #19
  br label %246

246:                                              ; preds = %245, %239
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %144) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %141) #19
  %247 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 0, i32 0
  %248 = load i8*, i8** %247, align 8, !tbaa !47
  %249 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %12, i64 0, i32 2
  %250 = bitcast %union.anon* %249 to i8*
  %251 = icmp eq i8* %248, %250
  br i1 %251, label %253, label %252

252:                                              ; preds = %246
  call void @_ZdlPv(i8* %248) #19
  br label %253

253:                                              ; preds = %252, %246
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %140) #19
  br label %274

254:                                              ; preds = %225, %136
  %255 = call noalias nonnull dereferenceable(88) i8* @_Znwm(i64 88) #21
  %256 = getelementptr inbounds i8, i8* %255, i64 40
  %257 = bitcast i8* %256 to %"class.NES::Runtime::detail::MemorySegment"**
  call void @llvm.memset.p0i8.i64(i8* nonnull align 4 dereferenceable(40) %255, i8 0, i64 40, i1 false) #19
  store %"class.NES::Runtime::detail::MemorySegment"* %0, %"class.NES::Runtime::detail::MemorySegment"** %257, align 8, !tbaa !12
  %258 = getelementptr inbounds i8, i8* %255, i64 48
  %259 = bitcast i8* %258 to %"class.NES::Runtime::BufferRecycler"**
  store %"class.NES::Runtime::BufferRecycler"* %3, %"class.NES::Runtime::BufferRecycler"** %259, align 8, !tbaa !19
  %260 = getelementptr inbounds i8, i8* %255, i64 56
  %261 = getelementptr inbounds i8, i8* %255, i64 72
  %262 = bitcast i8* %261 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  %263 = bitcast { i64, i64 }* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %263)
  %264 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %263, i8* nonnull align 8 dereferenceable(16) %264, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %264, i8* nonnull align 8 dereferenceable(16) %260, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %260, i8* nonnull align 8 dereferenceable(16) %263, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %263)
  %265 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  %266 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %265, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %265, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %266, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %262, align 8, !tbaa !24
  %267 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %268 = getelementptr inbounds i8, i8* %255, i64 80
  %269 = bitcast i8* %268 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  %270 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %267, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %270, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %269, align 8, !tbaa !24
  %271 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"** %18 to i8**
  store i8* %255, i8** %271, align 8, !tbaa !11
  %272 = bitcast i8* %255 to i32*
  %273 = cmpxchg i32* %272, i32 0, i32 1 seq_cst seq_cst
  ret void

274:                                              ; preds = %253, %135
  %275 = phi { i8*, i32 } [ %241, %253 ], [ %123, %135 ]
  resume { i8*, i32 } %275
}

declare dso_local void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32), %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32)) local_unnamed_addr #2

; Function Attrs: nobuiltin nofree allocsize(0)
declare dso_local nonnull i8* @_Znwm(i64) local_unnamed_addr #11

; Function Attrs: nofree nounwind uwtable willreturn mustprogress
define dso_local zeroext i1 @_ZN3NES7Runtime6detail18BufferControlBlock7prepareEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0) local_unnamed_addr #12 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %3 = cmpxchg i32* %2, i32 0, i32 1 seq_cst seq_cst
  %4 = extractvalue { i32, i1 } %3, 1
  ret i1 %4
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN3NES7Runtime6detail13MemorySegmentD2Ev(%"class.NES::Runtime::detail::MemorySegment"* nocapture nonnull dereferenceable(24) %0) unnamed_addr #10 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %2 = alloca %"class.std::__cxx11::basic_string", align 8
  %3 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %4 = alloca %"class.std::basic_ostream", align 8
  %5 = alloca %"class.std::__cxx11::basic_string", align 8
  %6 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 0
  %7 = load i8*, i8** %6, align 8, !tbaa !8
  %8 = icmp eq i8* %7, null
  br i1 %8, label %172, label %9

9:                                                ; preds = %1
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 2
  %11 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %10, align 8, !tbaa !11
  %12 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %11, i64 0, i32 0, i32 0, i32 0
  %13 = load atomic i32, i32* %12 seq_cst, align 4
  %14 = icmp eq i32 %13, 0
  br i1 %14, label %139, label %15

15:                                               ; preds = %9
  %16 = bitcast %"class.std::__cxx11::basic_string"* %2 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %16) #19
  invoke void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %2)
          to label %17 unwind label %107

17:                                               ; preds = %15
  %18 = bitcast %"class.std::__cxx11::basic_stringbuf"* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %18) #19
  %19 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !25
  %20 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 1
  %21 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 7
  %22 = bitcast i8** %20 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %22, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %21) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !25
  %23 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 1
  store i32 24, i32* %23, align 8, !tbaa !27
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2
  %25 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 2
  %26 = bitcast %"class.std::__cxx11::basic_string"* %24 to %union.anon**
  store %union.anon* %25, %union.anon** %26, align 8, !tbaa !32
  %27 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 1
  store i64 0, i64* %27, align 8, !tbaa !33
  %28 = bitcast %union.anon* %25 to i8*
  store i8 0, i8* %28, align 8, !tbaa !23
  %29 = bitcast %"class.std::basic_ostream"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %29) #19
  %30 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0
  %31 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1
  %32 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %31, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %32) #19
  %33 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %31, i64 0, i32 0, i32 0
  %34 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %34, align 8, !tbaa !34
  %35 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 2
  store i8 0, i8* %35, align 8, !tbaa !37
  %36 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 3
  store i8 0, i8* %36, align 1, !tbaa !38
  %37 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 4
  %38 = bitcast %"class.std::basic_streambuf"** %37 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %38, i8 0, i64 32, i1 false) #19
  %39 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %39, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %33, align 8, !tbaa !25
  %40 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %41 = getelementptr inbounds i8, i8* %29, i64 %40
  %42 = bitcast i8* %41 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %42, %"class.std::basic_streambuf"* nonnull %30)
          to label %45 unwind label %43

43:                                               ; preds = %17
  %44 = landingpad { i8*, i32 }
          catch i8* null
  br label %122

45:                                               ; preds = %17
  %46 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %4, i8* nonnull getelementptr inbounds ([32 x i8], [32 x i8]* @.str.9, i64 0, i64 0), i64 31)
          to label %47 unwind label %109

47:                                               ; preds = %45
  %48 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %4, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %49 unwind label %109

49:                                               ; preds = %47
  %50 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %4, i8* nonnull getelementptr inbounds ([42 x i8], [42 x i8]* @.str.10, i64 0, i64 0), i64 41)
          to label %51 unwind label %109

51:                                               ; preds = %49
  %52 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8) %4, i32 %13)
          to label %53 unwind label %109

53:                                               ; preds = %51
  %54 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %52, i8* nonnull getelementptr inbounds ([21 x i8], [21 x i8]* @.str.11, i64 0, i64 0), i64 20)
          to label %55 unwind label %109

55:                                               ; preds = %53
  %56 = bitcast %"class.std::__cxx11::basic_string"* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %56) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !68)
  %57 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 2
  %58 = bitcast %"class.std::__cxx11::basic_string"* %5 to %union.anon**
  store %union.anon* %57, %union.anon** %58, align 8, !tbaa !32, !alias.scope !68
  %59 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 1
  store i64 0, i64* %59, align 8, !tbaa !33, !alias.scope !68
  %60 = bitcast %union.anon* %57 to i8*
  store i8 0, i8* %60, align 8, !tbaa !23, !alias.scope !68
  %61 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 5
  %62 = load i8*, i8** %61, align 8, !tbaa !42, !noalias !68
  %63 = icmp eq i8* %62, null
  br i1 %63, label %86, label %64

64:                                               ; preds = %55
  %65 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 3
  %66 = load i8*, i8** %65, align 8, !tbaa !45, !noalias !68
  %67 = icmp ugt i8* %62, %66
  %68 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 0, i32 4
  %69 = load i8*, i8** %68, align 8, !tbaa !46
  br i1 %67, label %70, label %81

70:                                               ; preds = %64
  %71 = ptrtoint i8* %62 to i64
  %72 = ptrtoint i8* %69 to i64
  %73 = sub i64 %71, %72
  %74 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %5, i64 0, i64 0, i8* %69, i64 %73)
          to label %87 unwind label %75

75:                                               ; preds = %86, %81, %70
  %76 = landingpad { i8*, i32 }
          catch i8* null
  %77 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 0, i32 0
  %78 = load i8*, i8** %77, align 8, !tbaa !47, !alias.scope !68
  %79 = icmp eq i8* %78, %60
  br i1 %79, label %117, label %80

80:                                               ; preds = %75
  call void @_ZdlPv(i8* %78) #19
  br label %117

81:                                               ; preds = %64
  %82 = ptrtoint i8* %66 to i64
  %83 = ptrtoint i8* %69 to i64
  %84 = sub i64 %82, %83
  %85 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %5, i64 0, i64 0, i8* %69, i64 %84)
          to label %87 unwind label %75

86:                                               ; preds = %55
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %5, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %24)
          to label %87 unwind label %75

87:                                               ; preds = %86, %81, %70
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %5, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %2)
          to label %88 unwind label %111

88:                                               ; preds = %87
  %89 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 0, i32 0
  %90 = load i8*, i8** %89, align 8, !tbaa !47
  %91 = icmp eq i8* %90, %60
  br i1 %91, label %93, label %92

92:                                               ; preds = %88
  call void @_ZdlPv(i8* %90) #19
  br label %93

93:                                               ; preds = %92, %88
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #19
  %94 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %94) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %29) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !25
  %95 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 0, i32 0
  %96 = load i8*, i8** %95, align 8, !tbaa !47
  %97 = icmp eq i8* %96, %28
  br i1 %97, label %99, label %98

98:                                               ; preds = %93
  call void @_ZdlPv(i8* %96) #19
  br label %99

99:                                               ; preds = %98, %93
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %21) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %18) #19
  %100 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 0, i32 0
  %101 = load i8*, i8** %100, align 8, !tbaa !47
  %102 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 2
  %103 = bitcast %union.anon* %102 to i8*
  %104 = icmp eq i8* %101, %103
  br i1 %104, label %106, label %105

105:                                              ; preds = %99
  call void @_ZdlPv(i8* %101) #19
  br label %106

106:                                              ; preds = %105, %99
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %16) #19
  br label %139

107:                                              ; preds = %15
  %108 = landingpad { i8*, i32 }
          catch i8* null
  br label %136

109:                                              ; preds = %53, %51, %49, %47, %45
  %110 = landingpad { i8*, i32 }
          catch i8* null
  br label %119

111:                                              ; preds = %87
  %112 = landingpad { i8*, i32 }
          catch i8* null
  %113 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %5, i64 0, i32 0, i32 0
  %114 = load i8*, i8** %113, align 8, !tbaa !47
  %115 = icmp eq i8* %114, %60
  br i1 %115, label %117, label %116

116:                                              ; preds = %111
  call void @_ZdlPv(i8* %114) #19
  br label %117

117:                                              ; preds = %116, %111, %80, %75
  %118 = phi { i8*, i32 } [ %76, %80 ], [ %76, %75 ], [ %112, %111 ], [ %112, %116 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %56) #19
  br label %119

119:                                              ; preds = %117, %109
  %120 = phi { i8*, i32 } [ %118, %117 ], [ %110, %109 ]
  %121 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %4, i64 0, i32 1, i32 0
  br label %122

122:                                              ; preds = %119, %43
  %123 = phi %"class.std::ios_base"* [ %32, %43 ], [ %121, %119 ]
  %124 = phi { i8*, i32 } [ %44, %43 ], [ %120, %119 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %123) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %29) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !25
  %125 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %3, i64 0, i32 2, i32 0, i32 0
  %126 = load i8*, i8** %125, align 8, !tbaa !47
  %127 = icmp eq i8* %126, %28
  br i1 %127, label %129, label %128

128:                                              ; preds = %122
  call void @_ZdlPv(i8* %126) #19
  br label %129

129:                                              ; preds = %128, %122
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %19, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %21) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %18) #19
  %130 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 0, i32 0
  %131 = load i8*, i8** %130, align 8, !tbaa !47
  %132 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %2, i64 0, i32 2
  %133 = bitcast %union.anon* %132 to i8*
  %134 = icmp eq i8* %131, %133
  br i1 %134, label %136, label %135

135:                                              ; preds = %129
  call void @_ZdlPv(i8* %131) #19
  br label %136

136:                                              ; preds = %135, %129, %107
  %137 = phi { i8*, i32 } [ %108, %107 ], [ %124, %129 ], [ %124, %135 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %16) #19
  %138 = extractvalue { i8*, i32 } %137, 0
  call void @__clang_call_terminate(i8* %138) #22
  unreachable

139:                                              ; preds = %106, %9
  %140 = getelementptr inbounds %"class.NES::Runtime::detail::MemorySegment", %"class.NES::Runtime::detail::MemorySegment"* %0, i64 0, i32 1
  %141 = load i32, i32* %140, align 8, !tbaa !10
  %142 = zext i32 %141 to i64
  %143 = load i8*, i8** %6, align 8, !tbaa !8
  %144 = getelementptr inbounds i8, i8* %143, i64 %142
  %145 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %10, align 8, !tbaa !11
  %146 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"* %145 to i8*
  %147 = icmp eq i8* %144, %146
  br i1 %147, label %161, label %148

148:                                              ; preds = %139
  %149 = icmp eq %"class.NES::Runtime::detail::BufferControlBlock"* %145, null
  br i1 %149, label %171, label %150

150:                                              ; preds = %148
  %151 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %145, i64 0, i32 8, i32 0, i32 1
  %152 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %151, align 8, !tbaa !20
  %153 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %152, null
  br i1 %153, label %160, label %154

154:                                              ; preds = %150
  %155 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %145, i64 0, i32 8, i32 0, i32 0
  %156 = invoke zeroext i1 %152(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %155, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %155, i32 3)
          to label %160 unwind label %157

157:                                              ; preds = %154
  %158 = landingpad { i8*, i32 }
          catch i8* null
  %159 = extractvalue { i8*, i32 } %158, 0
  call void @__clang_call_terminate(i8* %159) #22
  unreachable

160:                                              ; preds = %154, %150
  call void @_ZdlPv(i8* nonnull %146) #23
  br label %171

161:                                              ; preds = %139
  %162 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %145, i64 0, i32 8, i32 0, i32 1
  %163 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %162, align 8, !tbaa !20
  %164 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %163, null
  br i1 %164, label %171, label %165

165:                                              ; preds = %161
  %166 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %145, i64 0, i32 8, i32 0, i32 0
  %167 = invoke zeroext i1 %163(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %166, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %166, i32 3)
          to label %171 unwind label %168

168:                                              ; preds = %165
  %169 = landingpad { i8*, i32 }
          catch i8* null
  %170 = extractvalue { i8*, i32 } %169, 0
  call void @__clang_call_terminate(i8* %170) #22
  unreachable

171:                                              ; preds = %165, %161, %160, %148
  store i8* null, i8** %6, align 8, !tbaa !24
  br label %172

172:                                              ; preds = %171, %1
  ret void
}

declare dso_local nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZNSolsEi(%"class.std::basic_ostream"* nonnull dereferenceable(8), i32) local_unnamed_addr #2

; Function Attrs: noinline noreturn nounwind
define linkonce_odr hidden void @__clang_call_terminate(i8* %0) local_unnamed_addr #13 comdat {
  %2 = tail call i8* @__cxa_begin_catch(i8* %0) #19
  tail call void @_ZSt9terminatev() #22
  unreachable
}

declare dso_local i8* @__cxa_begin_catch(i8*) local_unnamed_addr

declare dso_local void @_ZSt9terminatev() local_unnamed_addr

; Function Attrs: nofree norecurse nounwind uwtable willreturn mustprogress
define dso_local i32 @_ZNK3NES7Runtime6detail18BufferControlBlock17getReferenceCountEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #14 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %3 = load atomic i32, i32* %2 seq_cst, align 4
  ret i32 %3
}

; Function Attrs: nofree nounwind uwtable willreturn
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlockC2EPNS1_13MemorySegmentEPNS0_14BufferRecyclerEOSt8functionIFvS4_S6_EE(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, %"class.NES::Runtime::detail::MemorySegment"* %1, %"class.NES::Runtime::BufferRecycler"* %2, %"class.std::function"* nocapture nonnull align 8 dereferenceable(32) %3) unnamed_addr #5 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %5 = alloca { i64, i64 }, align 8
  %6 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  %7 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"* %0 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 4 dereferenceable(40) %7, i8 0, i64 40, i1 false)
  store %"class.NES::Runtime::detail::MemorySegment"* %1, %"class.NES::Runtime::detail::MemorySegment"** %6, align 8, !tbaa !12
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7, i32 0, i32 0
  store %"class.NES::Runtime::BufferRecycler"* %2, %"class.NES::Runtime::BufferRecycler"** %8, align 8, !tbaa !19
  %9 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !20
  %11 = bitcast { i64, i64 }* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %11)
  %12 = bitcast %"class.std::function"* %3 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %11, i8* nonnull align 8 dereferenceable(16) %12, i64 16, i1 false) #19, !tbaa.struct !22
  %13 = bitcast %"class.std::function"* %9 to i8*
  tail call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %12, i8* nonnull align 8 dereferenceable(16) %13, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %13, i8* nonnull align 8 dereferenceable(16) %11, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %11)
  %14 = getelementptr inbounds %"class.std::function", %"class.std::function"* %3, i64 0, i32 0, i32 1
  %15 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !24
  %16 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %16, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %15, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %10, align 8, !tbaa !24
  %17 = getelementptr inbounds %"class.std::function", %"class.std::function"* %3, i64 0, i32 1
  %18 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %19 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %17, align 8, !tbaa !24
  %20 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %18, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %20, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %17, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %19, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %18, align 8, !tbaa !24
  ret void
}

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlockC2ERKS2_(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %0, %"class.NES::Runtime::detail::BufferControlBlock"* nonnull align 8 dereferenceable(88) %1) unnamed_addr #1 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
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
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !20
  %12 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 0, i32 0, i32 0
  %13 = bitcast %"class.NES::Runtime::detail::BufferControlBlock"* %0 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 4 dereferenceable(40) %13, i8 0, i64 40, i1 false)
  %14 = load atomic i32, i32* %12 seq_cst, align 8
  store atomic i32 %14, i32* %5 seq_cst, align 4
  %15 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 1
  %16 = load i32, i32* %15, align 4, !tbaa !71
  store i32 %16, i32* %6, align 4, !tbaa !71
  %17 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 4
  %18 = load i64, i64* %17, align 8, !tbaa !72
  store i64 %18, i64* %7, align 8, !tbaa !72
  %19 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %19) #19
  %20 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %20, align 8, !tbaa !20
  %21 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 1
  %22 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %21, align 8, !tbaa !20
  %23 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %22, null
  br i1 %23, label %41, label %24

24:                                               ; preds = %2
  %25 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %26 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 0
  %27 = invoke zeroext i1 %22(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %25, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %26, i32 2)
          to label %28 unwind label %32

28:                                               ; preds = %24
  %29 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 1
  %30 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %29, align 8, !tbaa !73
  %31 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %21, align 8, !tbaa !20
  br label %41

32:                                               ; preds = %24
  %33 = landingpad { i8*, i32 }
          cleanup
  %34 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %20, align 8, !tbaa !20
  %35 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %34, null
  br i1 %35, label %67, label %36

36:                                               ; preds = %32
  %37 = invoke zeroext i1 %34(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %25, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %25, i32 3)
          to label %67 unwind label %38

38:                                               ; preds = %36
  %39 = landingpad { i8*, i32 }
          catch i8* null
  %40 = extractvalue { i8*, i32 } %39, 0
  call void @__clang_call_terminate(i8* %40) #22
  unreachable

41:                                               ; preds = %28, %2
  %42 = phi void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* [ undef, %2 ], [ %30, %28 ]
  %43 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ null, %2 ], [ %31, %28 ]
  %44 = bitcast { i64, i64 }* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %44)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %44, i8* nonnull align 8 dereferenceable(16) %19, i64 16, i1 false) #19, !tbaa.struct !22
  %45 = bitcast %"class.std::function"* %10 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %19, i8* nonnull align 8 dereferenceable(16) %45, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %45, i8* nonnull align 8 dereferenceable(16) %44, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %44)
  %46 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %46, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %20, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %43, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !24
  %47 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %48 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %49 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %48, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %49, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %47, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %42, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %48, align 8, !tbaa !24
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
  call void @__clang_call_terminate(i8* %56) #22
  unreachable

57:                                               ; preds = %51, %41
  %58 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 5
  %59 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %19) #19
  %60 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 6
  %61 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %60, align 8, !tbaa !12
  %62 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  store %"class.NES::Runtime::detail::MemorySegment"* %61, %"class.NES::Runtime::detail::MemorySegment"** %62, align 8, !tbaa !12
  %63 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 2
  %64 = load i64, i64* %63, align 8, !tbaa !74
  store i64 %64, i64* %59, align 8, !tbaa !74
  %65 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 5
  %66 = load i64, i64* %65, align 8, !tbaa !75
  store i64 %66, i64* %58, align 8, !tbaa !75
  ret void

67:                                               ; preds = %36, %32
  %68 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %11, align 8, !tbaa !20
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
  call void @__clang_call_terminate(i8* %75) #22
  unreachable

76:                                               ; preds = %70, %67
  resume { i8*, i32 } %33
}

; Function Attrs: uwtable
define dso_local nonnull align 8 dereferenceable(88) %"class.NES::Runtime::detail::BufferControlBlock"* @_ZN3NES7Runtime6detail18BufferControlBlockaSERKS2_(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull returned dereferenceable(88) %0, %"class.NES::Runtime::detail::BufferControlBlock"* nonnull align 8 dereferenceable(88) %1) local_unnamed_addr #1 align 2 personality i32 (...)* @__gxx_personality_v0 {
  %3 = alloca { i64, i64 }, align 8
  %4 = alloca %"class.std::function", align 8
  %5 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 0, i32 0, i32 0
  %6 = load atomic i32, i32* %5 seq_cst, align 8
  %7 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  store atomic i32 %6, i32* %7 seq_cst, align 4
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 1
  %9 = load i32, i32* %8, align 4, !tbaa !71
  %10 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  store i32 %9, i32* %10, align 4, !tbaa !71
  %11 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %12 = bitcast %"class.std::function"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %12) #19
  %13 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !20
  %14 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 1
  %15 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !20
  %16 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %15, null
  br i1 %16, label %35, label %17

17:                                               ; preds = %2
  %18 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 0, i32 0
  %19 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 0, i32 0
  %20 = invoke zeroext i1 %15(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %19, i32 2)
          to label %21 unwind label %25

21:                                               ; preds = %17
  %22 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 8, i32 1
  %23 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %22, align 8, !tbaa !73
  %24 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !20
  br label %35

25:                                               ; preds = %17
  %26 = landingpad { i8*, i32 }
          cleanup
  %27 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !20
  %28 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %27, null
  br i1 %28, label %34, label %29

29:                                               ; preds = %25
  %30 = invoke zeroext i1 %27(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %18, i32 3)
          to label %34 unwind label %31

31:                                               ; preds = %29
  %32 = landingpad { i8*, i32 }
          catch i8* null
  %33 = extractvalue { i8*, i32 } %32, 0
  call void @__clang_call_terminate(i8* %33) #22
  unreachable

34:                                               ; preds = %29, %25
  resume { i8*, i32 } %26

35:                                               ; preds = %21, %2
  %36 = phi void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* [ undef, %2 ], [ %23, %21 ]
  %37 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ null, %2 ], [ %24, %21 ]
  %38 = bitcast { i64, i64 }* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %38)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %38, i8* nonnull align 8 dereferenceable(16) %12, i64 16, i1 false) #19, !tbaa.struct !22
  %39 = bitcast %"class.std::function"* %11 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %12, i8* nonnull align 8 dereferenceable(16) %39, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %39, i8* nonnull align 8 dereferenceable(16) %38, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %38)
  %40 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  %41 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %41, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %37, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !24
  %42 = getelementptr inbounds %"class.std::function", %"class.std::function"* %4, i64 0, i32 1
  %43 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %44 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %43, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %44, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %42, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %36, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %43, align 8, !tbaa !24
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
  call void @__clang_call_terminate(i8* %51) #22
  unreachable

52:                                               ; preds = %46, %35
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %12) #19
  %53 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 6
  %54 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %53, align 8, !tbaa !12
  %55 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  store %"class.NES::Runtime::detail::MemorySegment"* %54, %"class.NES::Runtime::detail::MemorySegment"** %55, align 8, !tbaa !12
  %56 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 2
  %57 = load i64, i64* %56, align 8, !tbaa !74
  %58 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  store i64 %57, i64* %58, align 8, !tbaa !74
  %59 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %1, i64 0, i32 4
  %60 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  %61 = bitcast i64* %59 to <2 x i64>*
  %62 = load <2 x i64>, <2 x i64>* %61, align 8, !tbaa !76
  %63 = bitcast i64* %60 to <2 x i64>*
  store <2 x i64> %62, <2 x i64>* %63, align 8, !tbaa !76
  ret %"class.NES::Runtime::detail::BufferControlBlock"* %0
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local %"class.NES::Runtime::detail::MemorySegment"* @_ZNK3NES7Runtime6detail18BufferControlBlock8getOwnerEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #15 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  %3 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %2, align 8, !tbaa !12
  ret %"class.NES::Runtime::detail::MemorySegment"* %3
}

; Function Attrs: uwtable
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock19resetBufferRecyclerEPNS0_14BufferRecyclerE(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, %"class.NES::Runtime::BufferRecycler"* %1) local_unnamed_addr #1 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
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
  %16 = atomicrmw xchg i64* %14, i64 %15 seq_cst
  br label %135

17:                                               ; preds = %2
  %18 = bitcast %"class.std::__cxx11::basic_string"* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %18) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %3)
  %19 = bitcast %"class.std::__cxx11::basic_stringbuf"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %19) #19
  %20 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !25
  %21 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 1
  %22 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 7
  %23 = bitcast i8** %21 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %23, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %22) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !25
  %24 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 1
  store i32 24, i32* %24, align 8, !tbaa !27
  %25 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2
  %26 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 2
  %27 = bitcast %"class.std::__cxx11::basic_string"* %25 to %union.anon**
  store %union.anon* %26, %union.anon** %27, align 8, !tbaa !32
  %28 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 1
  store i64 0, i64* %28, align 8, !tbaa !33
  %29 = bitcast %union.anon* %26 to i8*
  store i8 0, i8* %29, align 8, !tbaa !23
  %30 = bitcast %"class.std::basic_ostream"* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %30) #19
  %31 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0
  %32 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1
  %33 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %32, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %33) #19
  %34 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %32, i64 0, i32 0, i32 0
  %35 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %35, align 8, !tbaa !34
  %36 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 2
  store i8 0, i8* %36, align 8, !tbaa !37
  %37 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 3
  store i8 0, i8* %37, align 1, !tbaa !38
  %38 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 4
  %39 = bitcast %"class.std::basic_streambuf"** %38 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %39, i8 0, i64 32, i1 false) #19
  %40 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %40, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  %41 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %42 = getelementptr inbounds i8, i8* %30, i64 %41
  %43 = bitcast i8* %42 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %43, %"class.std::basic_streambuf"* nonnull %31)
          to label %46 unwind label %44

44:                                               ; preds = %17
  %45 = landingpad { i8*, i32 }
          cleanup
  br label %120

46:                                               ; preds = %17
  %47 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %5, i8* nonnull getelementptr inbounds ([29 x i8], [29 x i8]* @.str.12, i64 0, i64 0), i64 28)
          to label %48 unwind label %107

48:                                               ; preds = %46
  %49 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %5, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %50 unwind label %107

50:                                               ; preds = %48
  %51 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %5, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.13, i64 0, i64 0), i64 16)
          to label %52 unwind label %107

52:                                               ; preds = %50
  %53 = bitcast %"class.std::__cxx11::basic_string"* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %53) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !77)
  %54 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 2
  %55 = bitcast %"class.std::__cxx11::basic_string"* %6 to %union.anon**
  store %union.anon* %54, %union.anon** %55, align 8, !tbaa !32, !alias.scope !77
  %56 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 1
  store i64 0, i64* %56, align 8, !tbaa !33, !alias.scope !77
  %57 = bitcast %union.anon* %54 to i8*
  store i8 0, i8* %57, align 8, !tbaa !23, !alias.scope !77
  %58 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 5
  %59 = load i8*, i8** %58, align 8, !tbaa !42, !noalias !77
  %60 = icmp eq i8* %59, null
  br i1 %60, label %83, label %61

61:                                               ; preds = %52
  %62 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 3
  %63 = load i8*, i8** %62, align 8, !tbaa !45, !noalias !77
  %64 = icmp ugt i8* %59, %63
  %65 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 0, i32 4
  %66 = load i8*, i8** %65, align 8, !tbaa !46
  br i1 %64, label %67, label %78

67:                                               ; preds = %61
  %68 = ptrtoint i8* %59 to i64
  %69 = ptrtoint i8* %66 to i64
  %70 = sub i64 %68, %69
  %71 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %6, i64 0, i64 0, i8* %66, i64 %70)
          to label %84 unwind label %72

72:                                               ; preds = %83, %78, %67
  %73 = landingpad { i8*, i32 }
          cleanup
  %74 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 0, i32 0
  %75 = load i8*, i8** %74, align 8, !tbaa !47, !alias.scope !77
  %76 = icmp eq i8* %75, %57
  br i1 %76, label %115, label %77

77:                                               ; preds = %72
  call void @_ZdlPv(i8* %75) #19
  br label %115

78:                                               ; preds = %61
  %79 = ptrtoint i8* %63 to i64
  %80 = ptrtoint i8* %66 to i64
  %81 = sub i64 %79, %80
  %82 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %6, i64 0, i64 0, i8* %66, i64 %81)
          to label %84 unwind label %72

83:                                               ; preds = %52
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %6, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %25)
          to label %84 unwind label %72

84:                                               ; preds = %83, %78, %67
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %6, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %3)
          to label %85 unwind label %109

85:                                               ; preds = %84
  %86 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 0, i32 0
  %87 = load i8*, i8** %86, align 8, !tbaa !47
  %88 = icmp eq i8* %87, %57
  br i1 %88, label %90, label %89

89:                                               ; preds = %85
  call void @_ZdlPv(i8* %87) #19
  br label %90

90:                                               ; preds = %89, %85
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %53) #19
  %91 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %91) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %30) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !25
  %92 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 0, i32 0
  %93 = load i8*, i8** %92, align 8, !tbaa !47
  %94 = icmp eq i8* %93, %29
  br i1 %94, label %96, label %95

95:                                               ; preds = %90
  call void @_ZdlPv(i8* %93) #19
  br label %96

96:                                               ; preds = %95, %90
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %22) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %19) #19
  %97 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 0, i32 0
  %98 = load i8*, i8** %97, align 8, !tbaa !47
  %99 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 2
  %100 = bitcast %union.anon* %99 to i8*
  %101 = icmp eq i8* %98, %100
  br i1 %101, label %103, label %102

102:                                              ; preds = %96
  call void @_ZdlPv(i8* %98) #19
  br label %103

103:                                              ; preds = %102, %96
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %18) #19
  %104 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7
  %105 = bitcast %"struct.std::atomic.0"* %104 to i64*
  %106 = atomicrmw xchg i64* %105, i64 0 seq_cst
  br label %135

107:                                              ; preds = %50, %48, %46
  %108 = landingpad { i8*, i32 }
          cleanup
  br label %117

109:                                              ; preds = %84
  %110 = landingpad { i8*, i32 }
          cleanup
  %111 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %6, i64 0, i32 0, i32 0
  %112 = load i8*, i8** %111, align 8, !tbaa !47
  %113 = icmp eq i8* %112, %57
  br i1 %113, label %115, label %114

114:                                              ; preds = %109
  call void @_ZdlPv(i8* %112) #19
  br label %115

115:                                              ; preds = %114, %109, %77, %72
  %116 = phi { i8*, i32 } [ %73, %77 ], [ %73, %72 ], [ %110, %109 ], [ %110, %114 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %53) #19
  br label %117

117:                                              ; preds = %115, %107
  %118 = phi { i8*, i32 } [ %116, %115 ], [ %108, %107 ]
  %119 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %5, i64 0, i32 1, i32 0
  br label %120

120:                                              ; preds = %117, %44
  %121 = phi %"class.std::ios_base"* [ %33, %44 ], [ %119, %117 ]
  %122 = phi { i8*, i32 } [ %45, %44 ], [ %118, %117 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %121) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %30) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !25
  %123 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %4, i64 0, i32 2, i32 0, i32 0
  %124 = load i8*, i8** %123, align 8, !tbaa !47
  %125 = icmp eq i8* %124, %29
  br i1 %125, label %127, label %126

126:                                              ; preds = %120
  call void @_ZdlPv(i8* %124) #19
  br label %127

127:                                              ; preds = %126, %120
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %20, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %22) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %19) #19
  %128 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 0, i32 0
  %129 = load i8*, i8** %128, align 8, !tbaa !47
  %130 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %3, i64 0, i32 2
  %131 = bitcast %union.anon* %130 to i8*
  %132 = icmp eq i8* %129, %131
  br i1 %132, label %134, label %133

133:                                              ; preds = %127
  call void @_ZdlPv(i8* %129) #19
  br label %134

134:                                              ; preds = %133, %127
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %18) #19
  br label %255

135:                                              ; preds = %103, %12
  %136 = phi i64 [ %106, %103 ], [ %16, %12 ]
  %137 = inttoptr i64 %136 to %"class.NES::Runtime::BufferRecycler"*
  %138 = icmp eq %"class.NES::Runtime::BufferRecycler"* %137, %1
  br i1 %138, label %139, label %254

139:                                              ; preds = %135
  %140 = bitcast %"class.std::__cxx11::basic_string"* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %140) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %7)
  %141 = bitcast %"class.std::__cxx11::basic_stringbuf"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %141) #19
  %142 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %143 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 1
  %144 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 7
  %145 = bitcast i8** %143 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %145, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %144) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %146 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 1
  store i32 24, i32* %146, align 8, !tbaa !27
  %147 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2
  %148 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 2
  %149 = bitcast %"class.std::__cxx11::basic_string"* %147 to %union.anon**
  store %union.anon* %148, %union.anon** %149, align 8, !tbaa !32
  %150 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 1
  store i64 0, i64* %150, align 8, !tbaa !33
  %151 = bitcast %union.anon* %148 to i8*
  store i8 0, i8* %151, align 8, !tbaa !23
  %152 = bitcast %"class.std::basic_ostream"* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %152) #19
  %153 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0
  %154 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1
  %155 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %154, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %155) #19
  %156 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %154, i64 0, i32 0, i32 0
  %157 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %157, align 8, !tbaa !34
  %158 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 2
  store i8 0, i8* %158, align 8, !tbaa !37
  %159 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 3
  store i8 0, i8* %159, align 1, !tbaa !38
  %160 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 4
  %161 = bitcast %"class.std::basic_streambuf"** %160 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %161, i8 0, i64 32, i1 false) #19
  %162 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %162, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %156, align 8, !tbaa !25
  %163 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %164 = getelementptr inbounds i8, i8* %152, i64 %163
  %165 = bitcast i8* %164 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %165, %"class.std::basic_streambuf"* nonnull %153)
          to label %168 unwind label %166

166:                                              ; preds = %139
  %167 = landingpad { i8*, i32 }
          cleanup
  br label %239

168:                                              ; preds = %139
  %169 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([44 x i8], [44 x i8]* @.str.14, i64 0, i64 0), i64 43)
          to label %170 unwind label %226

170:                                              ; preds = %168
  %171 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %172 unwind label %226

172:                                              ; preds = %170
  %173 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %9, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.13, i64 0, i64 0), i64 16)
          to label %174 unwind label %226

174:                                              ; preds = %172
  %175 = bitcast %"class.std::__cxx11::basic_string"* %10 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %175) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !80)
  %176 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 2
  %177 = bitcast %"class.std::__cxx11::basic_string"* %10 to %union.anon**
  store %union.anon* %176, %union.anon** %177, align 8, !tbaa !32, !alias.scope !80
  %178 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 1
  store i64 0, i64* %178, align 8, !tbaa !33, !alias.scope !80
  %179 = bitcast %union.anon* %176 to i8*
  store i8 0, i8* %179, align 8, !tbaa !23, !alias.scope !80
  %180 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 5
  %181 = load i8*, i8** %180, align 8, !tbaa !42, !noalias !80
  %182 = icmp eq i8* %181, null
  br i1 %182, label %205, label %183

183:                                              ; preds = %174
  %184 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 3
  %185 = load i8*, i8** %184, align 8, !tbaa !45, !noalias !80
  %186 = icmp ugt i8* %181, %185
  %187 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 0, i32 4
  %188 = load i8*, i8** %187, align 8, !tbaa !46
  br i1 %186, label %189, label %200

189:                                              ; preds = %183
  %190 = ptrtoint i8* %181 to i64
  %191 = ptrtoint i8* %188 to i64
  %192 = sub i64 %190, %191
  %193 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, i64 0, i64 0, i8* %188, i64 %192)
          to label %206 unwind label %194

194:                                              ; preds = %205, %200, %189
  %195 = landingpad { i8*, i32 }
          cleanup
  %196 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %197 = load i8*, i8** %196, align 8, !tbaa !47, !alias.scope !80
  %198 = icmp eq i8* %197, %179
  br i1 %198, label %234, label %199

199:                                              ; preds = %194
  call void @_ZdlPv(i8* %197) #19
  br label %234

200:                                              ; preds = %183
  %201 = ptrtoint i8* %185 to i64
  %202 = ptrtoint i8* %188 to i64
  %203 = sub i64 %201, %202
  %204 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, i64 0, i64 0, i8* %188, i64 %203)
          to label %206 unwind label %194

205:                                              ; preds = %174
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %147)
          to label %206 unwind label %194

206:                                              ; preds = %205, %200, %189
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %10, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %7)
          to label %207 unwind label %228

207:                                              ; preds = %206
  %208 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %209 = load i8*, i8** %208, align 8, !tbaa !47
  %210 = icmp eq i8* %209, %179
  br i1 %210, label %212, label %211

211:                                              ; preds = %207
  call void @_ZdlPv(i8* %209) #19
  br label %212

212:                                              ; preds = %211, %207
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %175) #19
  %213 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %213) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %152) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %214 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 0, i32 0
  %215 = load i8*, i8** %214, align 8, !tbaa !47
  %216 = icmp eq i8* %215, %151
  br i1 %216, label %218, label %217

217:                                              ; preds = %212
  call void @_ZdlPv(i8* %215) #19
  br label %218

218:                                              ; preds = %217, %212
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %144) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %141) #19
  %219 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %220 = load i8*, i8** %219, align 8, !tbaa !47
  %221 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %222 = bitcast %union.anon* %221 to i8*
  %223 = icmp eq i8* %220, %222
  br i1 %223, label %225, label %224

224:                                              ; preds = %218
  call void @_ZdlPv(i8* %220) #19
  br label %225

225:                                              ; preds = %224, %218
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %140) #19
  br label %254

226:                                              ; preds = %172, %170, %168
  %227 = landingpad { i8*, i32 }
          cleanup
  br label %236

228:                                              ; preds = %206
  %229 = landingpad { i8*, i32 }
          cleanup
  %230 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %10, i64 0, i32 0, i32 0
  %231 = load i8*, i8** %230, align 8, !tbaa !47
  %232 = icmp eq i8* %231, %179
  br i1 %232, label %234, label %233

233:                                              ; preds = %228
  call void @_ZdlPv(i8* %231) #19
  br label %234

234:                                              ; preds = %233, %228, %199, %194
  %235 = phi { i8*, i32 } [ %195, %199 ], [ %195, %194 ], [ %229, %228 ], [ %229, %233 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %175) #19
  br label %236

236:                                              ; preds = %234, %226
  %237 = phi { i8*, i32 } [ %235, %234 ], [ %227, %226 ]
  %238 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %9, i64 0, i32 1, i32 0
  br label %239

239:                                              ; preds = %236, %166
  %240 = phi %"class.std::ios_base"* [ %155, %166 ], [ %238, %236 ]
  %241 = phi { i8*, i32 } [ %167, %166 ], [ %237, %236 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %240) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %152) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  %242 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %8, i64 0, i32 2, i32 0, i32 0
  %243 = load i8*, i8** %242, align 8, !tbaa !47
  %244 = icmp eq i8* %243, %151
  br i1 %244, label %246, label %245

245:                                              ; preds = %239
  call void @_ZdlPv(i8* %243) #19
  br label %246

246:                                              ; preds = %245, %239
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %142, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %144) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %141) #19
  %247 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %248 = load i8*, i8** %247, align 8, !tbaa !47
  %249 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %250 = bitcast %union.anon* %249 to i8*
  %251 = icmp eq i8* %248, %250
  br i1 %251, label %253, label %252

252:                                              ; preds = %246
  call void @_ZdlPv(i8* %248) #19
  br label %253

253:                                              ; preds = %252, %246
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %140) #19
  br label %255

254:                                              ; preds = %225, %135
  ret void

255:                                              ; preds = %253, %134
  %256 = phi { i8*, i32 } [ %241, %253 ], [ %122, %134 ]
  resume { i8*, i32 } %256
}

; Function Attrs: nounwind uwtable
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEE(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %0, %"class.std::function"* nonnull align 8 dereferenceable(32) %1) local_unnamed_addr #10 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %3 = alloca { i64, i64 }, align 8
  %4 = alloca { i64, i64 }, align 8
  %5 = alloca { i64, i64 }, align 8
  %6 = alloca %"class.std::function", align 8
  %7 = alloca %class.anon, align 8
  %8 = alloca %"class.std::function", align 8
  %9 = alloca %class.anon, align 8
  %10 = bitcast %"class.std::function"* %8 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %10) #19
  %11 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8
  %12 = getelementptr inbounds %"class.std::function", %"class.std::function"* %8, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %12, align 8, !tbaa !20
  %13 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  %14 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !20
  %15 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %14, null
  br i1 %15, label %16, label %19

16:                                               ; preds = %2
  %17 = bitcast %class.anon* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 64, i8* nonnull %17) #19
  %18 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %18, align 8, !tbaa !20
  br label %56

19:                                               ; preds = %2
  %20 = getelementptr inbounds %"class.std::function", %"class.std::function"* %8, i64 0, i32 0, i32 0
  %21 = getelementptr inbounds %"class.std::function", %"class.std::function"* %11, i64 0, i32 0, i32 0
  %22 = invoke zeroext i1 %14(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %21, i32 2)
          to label %32 unwind label %23

23:                                               ; preds = %19
  %24 = landingpad { i8*, i32 }
          catch i8* null
  %25 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %12, align 8, !tbaa !20
  %26 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %25, null
  br i1 %26, label %236, label %27

27:                                               ; preds = %23
  %28 = invoke zeroext i1 %25(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, i32 3)
          to label %236 unwind label %29

29:                                               ; preds = %27
  %30 = landingpad { i8*, i32 }
          catch i8* null
  %31 = extractvalue { i8*, i32 } %30, 0
  call void @__clang_call_terminate(i8* %31) #22
  unreachable

32:                                               ; preds = %19
  %33 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %34 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %33, align 8, !tbaa !73
  %35 = getelementptr inbounds %"class.std::function", %"class.std::function"* %8, i64 0, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %34, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %35, align 8, !tbaa !73
  %36 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !20
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %36, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %12, align 8, !tbaa !20
  %37 = bitcast %class.anon* %9 to i8*
  call void @llvm.lifetime.start.p0i8(i64 64, i8* nonnull %37) #19
  %38 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %38, align 8, !tbaa !20
  %39 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %36, null
  br i1 %39, label %56, label %40

40:                                               ; preds = %32
  %41 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 0
  %42 = invoke zeroext i1 %36(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %41, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, i32 2)
          to label %43 unwind label %47

43:                                               ; preds = %40
  %44 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %35, align 8, !tbaa !73
  %45 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %44, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %45, align 8, !tbaa !73
  %46 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %12, align 8, !tbaa !20
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %46, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %38, align 8, !tbaa !20
  br label %56

47:                                               ; preds = %40
  %48 = landingpad { i8*, i32 }
          catch i8* null
  %49 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %38, align 8, !tbaa !20
  %50 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %49, null
  br i1 %50, label %225, label %51

51:                                               ; preds = %47
  %52 = invoke zeroext i1 %49(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %41, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %41, i32 3)
          to label %225 unwind label %53

53:                                               ; preds = %51
  %54 = landingpad { i8*, i32 }
          catch i8* null
  %55 = extractvalue { i8*, i32 } %54, 0
  call void @__clang_call_terminate(i8* %55) #22
  unreachable

56:                                               ; preds = %43, %32, %16
  %57 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ null, %16 ], [ null, %32 ], [ %46, %43 ]
  %58 = phi i8* [ %17, %16 ], [ %37, %32 ], [ %37, %43 ]
  %59 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1
  %60 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %60, align 8, !tbaa !20
  %61 = getelementptr inbounds %"class.std::function", %"class.std::function"* %1, i64 0, i32 0, i32 1
  %62 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %61, align 8, !tbaa !20
  %63 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %62, null
  br i1 %63, label %64, label %67

64:                                               ; preds = %56
  %65 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 1
  %66 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %65, align 8, !tbaa !24
  br label %87

67:                                               ; preds = %56
  %68 = getelementptr inbounds %"class.std::function", %"class.std::function"* %59, i64 0, i32 0, i32 0
  %69 = getelementptr inbounds %"class.std::function", %"class.std::function"* %1, i64 0, i32 0, i32 0
  %70 = invoke zeroext i1 %62(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %68, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %69, i32 2)
          to label %71 unwind label %78

71:                                               ; preds = %67
  %72 = getelementptr inbounds %"class.std::function", %"class.std::function"* %1, i64 0, i32 1
  %73 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %72, align 8, !tbaa !73
  %74 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %73, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %74, align 8, !tbaa !73
  %75 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %61, align 8, !tbaa !20
  %76 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  %77 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %76, align 8, !tbaa !24
  br label %87

78:                                               ; preds = %67
  %79 = landingpad { i8*, i32 }
          catch i8* null
  %80 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %60, align 8, !tbaa !20
  %81 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %80, null
  br i1 %81, label %200, label %82

82:                                               ; preds = %78
  %83 = invoke zeroext i1 %80(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %68, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %68, i32 3)
          to label %200 unwind label %84

84:                                               ; preds = %82
  %85 = landingpad { i8*, i32 }
          catch i8* null
  %86 = extractvalue { i8*, i32 } %85, 0
  call void @__clang_call_terminate(i8* %86) #22
  unreachable

87:                                               ; preds = %71, %64
  %88 = phi void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* [ %73, %71 ], [ %66, %64 ]
  %89 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ %75, %71 ], [ null, %64 ]
  %90 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ %77, %71 ], [ %57, %64 ]
  %91 = bitcast %class.anon* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 64, i8* nonnull %91)
  %92 = bitcast %"class.std::function"* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %92) #19
  %93 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 1
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %91, i8* nonnull align 8 dereferenceable(16) %58, i64 16, i1 false) #19
  %94 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %94, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %90, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %93, align 8, !tbaa !24
  %95 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 1
  %96 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 1
  %97 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %95, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %97, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %96, align 8, !tbaa !24
  %98 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1
  %99 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 0, i32 1
  %100 = bitcast %"class.std::function"* %59 to i8*
  %101 = bitcast %"class.std::function"* %98 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %101, i8* nonnull align 8 dereferenceable(16) %100, i64 16, i1 false) #19
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %60, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %89, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %99, align 8, !tbaa !24
  %102 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 1
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %88, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %102, align 8, !tbaa !24
  %103 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 0, i32 1
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %103, align 8, !tbaa !20
  %104 = invoke noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #21
          to label %105 unwind label %149

105:                                              ; preds = %87
  %106 = getelementptr inbounds i8, i8* %104, i64 16
  %107 = bitcast i8* %106 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  %108 = bitcast { i64, i64 }* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %108)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %108, i8* nonnull align 8 dereferenceable(16) %91, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %91, i8* nonnull align 8 dereferenceable(16) %104, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %104, i8* nonnull align 8 dereferenceable(16) %108, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %108)
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %93, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %90, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %107, align 8, !tbaa !24
  %109 = getelementptr inbounds i8, i8* %104, i64 24
  %110 = bitcast i8* %109 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %97, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %110, align 8, !tbaa !24
  %111 = getelementptr inbounds i8, i8* %104, i64 32
  %112 = getelementptr inbounds i8, i8* %104, i64 48
  %113 = bitcast i8* %112 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  %114 = bitcast { i64, i64 }* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %114)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %114, i8* nonnull align 8 dereferenceable(16) %101, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %101, i8* nonnull align 8 dereferenceable(16) %111, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %111, i8* nonnull align 8 dereferenceable(16) %114, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %114)
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %99, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %89, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %113, align 8, !tbaa !24
  %115 = getelementptr inbounds i8, i8* %104, i64 56
  %116 = bitcast i8* %115 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %88, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %116, align 8, !tbaa !24
  %117 = bitcast %"class.std::function"* %6 to i8**
  store i8* %104, i8** %117, align 8, !tbaa !24
  %118 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 1
  %119 = bitcast { i64, i64 }* %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 16, i8* nonnull %119)
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %119, i8* nonnull align 8 dereferenceable(16) %92, i64 16, i1 false) #19, !tbaa.struct !22
  %120 = bitcast %"class.std::function"* %11 to i8*
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %92, i8* nonnull align 8 dereferenceable(16) %120, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.memcpy.p0i8.p0i8.i64(i8* nonnull align 8 dereferenceable(16) %120, i8* nonnull align 8 dereferenceable(16) %119, i64 16, i1 false) #19, !tbaa.struct !22
  call void @llvm.lifetime.end.p0i8(i64 16, i8* nonnull %119)
  %121 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %121, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %103, align 8, !tbaa !24
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* @"_ZNSt14_Function_base13_Base_managerIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS3_13MemorySegmentEPNS2_14BufferRecyclerEEEE3$_0E10_M_managerERSt9_Any_dataRKSF_St18_Manager_operation", i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %13, align 8, !tbaa !24
  %122 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %123 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %122, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %123, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %118, align 8, !tbaa !24
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* @"_ZNSt17_Function_handlerIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEZNS2_18BufferControlBlock18addRecycleCallbackEOSt8functionIS7_EE3$_0E9_M_invokeERKSt9_Any_dataOS4_OS6_", void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %122, align 8, !tbaa !24
  %124 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %121, null
  br i1 %124, label %140, label %125

125:                                              ; preds = %105
  %126 = getelementptr inbounds %"class.std::function", %"class.std::function"* %6, i64 0, i32 0, i32 0
  %127 = invoke zeroext i1 %121(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %126, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %126, i32 3)
          to label %131 unwind label %128

128:                                              ; preds = %125
  %129 = landingpad { i8*, i32 }
          catch i8* null
  %130 = extractvalue { i8*, i32 } %129, 0
  call void @__clang_call_terminate(i8* %130) #22
  unreachable

131:                                              ; preds = %125
  %132 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %99, align 8, !tbaa !20
  %133 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %132, null
  br i1 %133, label %140, label %134

134:                                              ; preds = %131
  %135 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 0, i32 0
  %136 = invoke zeroext i1 %132(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %135, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %135, i32 3)
          to label %140 unwind label %137

137:                                              ; preds = %134
  %138 = landingpad { i8*, i32 }
          catch i8* null
  %139 = extractvalue { i8*, i32 } %138, 0
  call void @__clang_call_terminate(i8* %139) #22
  unreachable

140:                                              ; preds = %134, %131, %105
  %141 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %93, align 8, !tbaa !20
  %142 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %141, null
  br i1 %142, label %172, label %143

143:                                              ; preds = %140
  %144 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 0
  %145 = invoke zeroext i1 %141(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %144, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %144, i32 3)
          to label %172 unwind label %146

146:                                              ; preds = %143
  %147 = landingpad { i8*, i32 }
          catch i8* null
  %148 = extractvalue { i8*, i32 } %147, 0
  call void @__clang_call_terminate(i8* %148) #22
  unreachable

149:                                              ; preds = %87
  %150 = landingpad { i8*, i32 }
          catch i8* null
  %151 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %89, null
  br i1 %151, label %160, label %152

152:                                              ; preds = %149
  %153 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 1, i32 0, i32 0
  %154 = invoke zeroext i1 %89(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %153, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %153, i32 3)
          to label %155 unwind label %157

155:                                              ; preds = %152
  %156 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %93, align 8, !tbaa !20
  br label %160

157:                                              ; preds = %152
  %158 = landingpad { i8*, i32 }
          catch i8* null
  %159 = extractvalue { i8*, i32 } %158, 0
  call void @__clang_call_terminate(i8* %159) #22
  unreachable

160:                                              ; preds = %155, %149
  %161 = phi i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* [ %156, %155 ], [ %90, %149 ]
  %162 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %161, null
  br i1 %162, label %169, label %163

163:                                              ; preds = %160
  %164 = getelementptr inbounds %class.anon, %class.anon* %7, i64 0, i32 0, i32 0, i32 0
  %165 = invoke zeroext i1 %161(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %164, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %164, i32 3)
          to label %169 unwind label %166

166:                                              ; preds = %163
  %167 = landingpad { i8*, i32 }
          catch i8* null
  %168 = extractvalue { i8*, i32 } %167, 0
  call void @__clang_call_terminate(i8* %168) #22
  unreachable

169:                                              ; preds = %163, %160
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %92) #19
  %170 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %60, align 8, !tbaa !20
  %171 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %170, null
  br i1 %171, label %216, label %210

172:                                              ; preds = %143, %140
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %92) #19
  call void @llvm.lifetime.end.p0i8(i64 64, i8* nonnull %91)
  %173 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %60, align 8, !tbaa !20
  %174 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %173, null
  br i1 %174, label %181, label %175

175:                                              ; preds = %172
  %176 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 0
  %177 = invoke zeroext i1 %173(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %176, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %176, i32 3)
          to label %181 unwind label %178

178:                                              ; preds = %175
  %179 = landingpad { i8*, i32 }
          catch i8* null
  %180 = extractvalue { i8*, i32 } %179, 0
  call void @__clang_call_terminate(i8* %180) #22
  unreachable

181:                                              ; preds = %175, %172
  %182 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %94, align 8, !tbaa !20
  %183 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %182, null
  br i1 %183, label %190, label %184

184:                                              ; preds = %181
  %185 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 0
  %186 = invoke zeroext i1 %182(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %185, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %185, i32 3)
          to label %190 unwind label %187

187:                                              ; preds = %184
  %188 = landingpad { i8*, i32 }
          catch i8* null
  %189 = extractvalue { i8*, i32 } %188, 0
  call void @__clang_call_terminate(i8* %189) #22
  unreachable

190:                                              ; preds = %184, %181
  call void @llvm.lifetime.end.p0i8(i64 64, i8* nonnull %58) #19
  %191 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %12, align 8, !tbaa !20
  %192 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %191, null
  br i1 %192, label %199, label %193

193:                                              ; preds = %190
  %194 = getelementptr inbounds %"class.std::function", %"class.std::function"* %8, i64 0, i32 0, i32 0
  %195 = invoke zeroext i1 %191(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %194, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %194, i32 3)
          to label %199 unwind label %196

196:                                              ; preds = %193
  %197 = landingpad { i8*, i32 }
          catch i8* null
  %198 = extractvalue { i8*, i32 } %197, 0
  call void @__clang_call_terminate(i8* %198) #22
  unreachable

199:                                              ; preds = %193, %190
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %10) #19
  ret void

200:                                              ; preds = %82, %78
  %201 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  %202 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %201, align 8, !tbaa !20
  %203 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %202, null
  br i1 %203, label %225, label %204

204:                                              ; preds = %200
  %205 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 0
  %206 = invoke zeroext i1 %202(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %205, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %205, i32 3)
          to label %225 unwind label %207

207:                                              ; preds = %204
  %208 = landingpad { i8*, i32 }
          catch i8* null
  %209 = extractvalue { i8*, i32 } %208, 0
  call void @__clang_call_terminate(i8* %209) #22
  unreachable

210:                                              ; preds = %169
  %211 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 0
  %212 = invoke zeroext i1 %170(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %211, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %211, i32 3)
          to label %216 unwind label %213

213:                                              ; preds = %210
  %214 = landingpad { i8*, i32 }
          catch i8* null
  %215 = extractvalue { i8*, i32 } %214, 0
  call void @__clang_call_terminate(i8* %215) #22
  unreachable

216:                                              ; preds = %210, %169
  %217 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %94, align 8, !tbaa !20
  %218 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %217, null
  br i1 %218, label %225, label %219

219:                                              ; preds = %216
  %220 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 0
  %221 = invoke zeroext i1 %217(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %220, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %220, i32 3)
          to label %225 unwind label %222

222:                                              ; preds = %219
  %223 = landingpad { i8*, i32 }
          catch i8* null
  %224 = extractvalue { i8*, i32 } %223, 0
  call void @__clang_call_terminate(i8* %224) #22
  unreachable

225:                                              ; preds = %219, %216, %204, %200, %51, %47
  %226 = phi i8* [ %37, %51 ], [ %37, %47 ], [ %58, %200 ], [ %58, %204 ], [ %58, %216 ], [ %58, %219 ]
  %227 = phi { i8*, i32 } [ %48, %51 ], [ %48, %47 ], [ %79, %200 ], [ %79, %204 ], [ %150, %216 ], [ %150, %219 ]
  call void @llvm.lifetime.end.p0i8(i64 64, i8* nonnull %226) #19
  %228 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %12, align 8, !tbaa !20
  %229 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %228, null
  br i1 %229, label %236, label %230

230:                                              ; preds = %225
  %231 = getelementptr inbounds %"class.std::function", %"class.std::function"* %8, i64 0, i32 0, i32 0
  %232 = invoke zeroext i1 %228(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %231, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %231, i32 3)
          to label %236 unwind label %233

233:                                              ; preds = %230
  %234 = landingpad { i8*, i32 }
          catch i8* null
  %235 = extractvalue { i8*, i32 } %234, 0
  call void @__clang_call_terminate(i8* %235) #22
  unreachable

236:                                              ; preds = %230, %225, %27, %23
  %237 = phi { i8*, i32 } [ %24, %27 ], [ %24, %23 ], [ %227, %225 ], [ %227, %230 ]
  %238 = extractvalue { i8*, i32 } %237, 0
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %10) #19
  call void @__clang_call_terminate(i8* %238) #22
  unreachable
}

; Function Attrs: uwtable
define internal zeroext i1 @"_ZNSt14_Function_base13_Base_managerIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS3_13MemorySegmentEPNS2_14BufferRecyclerEEEE3$_0E10_M_managerERSt9_Any_dataRKSF_St18_Manager_operation"(%"union.std::_Any_data"* nocapture nonnull align 8 dereferenceable(16) %0, %"union.std::_Any_data"* nocapture nonnull readonly align 8 dereferenceable(16) %1, i32 %2) #1 align 2 personality i32 (...)* @__gxx_personality_v0 {
  switch i32 %2, label %103 [
    i32 0, label %4
    i32 1, label %6
    i32 2, label %10
    i32 3, label %77
  ]

4:                                                ; preds = %3
  %5 = bitcast %"union.std::_Any_data"* %0 to %"class.std::type_info"**
  store %"class.std::type_info"* bitcast ({ i8*, i8* }* @"_ZTIZN3NES7Runtime6detail18BufferControlBlock18addRecycleCallbackEOSt8functionIFvPNS1_13MemorySegmentEPNS0_14BufferRecyclerEEEE3$_0" to %"class.std::type_info"*), %"class.std::type_info"** %5, align 8, !tbaa !24
  br label %103

6:                                                ; preds = %3
  %7 = bitcast %"union.std::_Any_data"* %1 to %class.anon**
  %8 = load %class.anon*, %class.anon** %7, align 8, !tbaa !24
  %9 = bitcast %"union.std::_Any_data"* %0 to %class.anon**
  store %class.anon* %8, %class.anon** %9, align 8, !tbaa !24
  br label %103

10:                                               ; preds = %3
  %11 = tail call noalias nonnull dereferenceable(64) i8* @_Znwm(i64 64) #21
  %12 = bitcast %"union.std::_Any_data"* %1 to %class.anon**
  %13 = load %class.anon*, %class.anon** %12, align 8, !tbaa !24
  %14 = getelementptr inbounds i8, i8* %11, i64 16
  %15 = bitcast i8* %14 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !20
  %16 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 0, i32 0, i32 1
  %17 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %16, align 8, !tbaa !20
  %18 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %17, null
  br i1 %18, label %38, label %19

19:                                               ; preds = %10
  %20 = bitcast i8* %11 to %"union.std::_Any_data"*
  %21 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 0, i32 0, i32 0
  %22 = invoke zeroext i1 %17(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %21, i32 2)
          to label %23 unwind label %29

23:                                               ; preds = %19
  %24 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 0, i32 1
  %25 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %24, align 8, !tbaa !73
  %26 = getelementptr inbounds i8, i8* %11, i64 24
  %27 = bitcast i8* %26 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %25, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %27, align 8, !tbaa !73
  %28 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %16, align 8, !tbaa !20
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %28, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !20
  br label %38

29:                                               ; preds = %19
  %30 = landingpad { i8*, i32 }
          cleanup
  %31 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !20
  %32 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %31, null
  br i1 %32, label %73, label %33

33:                                               ; preds = %29
  %34 = invoke zeroext i1 %31(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %20, i32 3)
          to label %73 unwind label %35

35:                                               ; preds = %33
  %36 = landingpad { i8*, i32 }
          catch i8* null
  %37 = extractvalue { i8*, i32 } %36, 0
  tail call void @__clang_call_terminate(i8* %37) #22
  unreachable

38:                                               ; preds = %23, %10
  %39 = getelementptr inbounds i8, i8* %11, i64 48
  %40 = bitcast i8* %39 to i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)**
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* null, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !20
  %41 = getelementptr inbounds %class.anon, %class.anon* %13, i64 0, i32 1, i32 0, i32 1
  %42 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %41, align 8, !tbaa !20
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
  %51 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %50, align 8, !tbaa !73
  %52 = getelementptr inbounds i8, i8* %11, i64 56
  %53 = bitcast i8* %52 to void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)**
  store void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* %51, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %53, align 8, !tbaa !73
  %54 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %41, align 8, !tbaa !20
  store i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %54, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !20
  br label %75

55:                                               ; preds = %44
  %56 = landingpad { i8*, i32 }
          cleanup
  %57 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %40, align 8, !tbaa !20
  %58 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %57, null
  br i1 %58, label %64, label %59

59:                                               ; preds = %55
  %60 = invoke zeroext i1 %57(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %46, %"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %46, i32 3)
          to label %64 unwind label %61

61:                                               ; preds = %59
  %62 = landingpad { i8*, i32 }
          catch i8* null
  %63 = extractvalue { i8*, i32 } %62, 0
  tail call void @__clang_call_terminate(i8* %63) #22
  unreachable

64:                                               ; preds = %59, %55
  %65 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %15, align 8, !tbaa !20
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
  tail call void @__clang_call_terminate(i8* %72) #22
  unreachable

73:                                               ; preds = %67, %64, %33, %29
  %74 = phi { i8*, i32 } [ %30, %33 ], [ %30, %29 ], [ %56, %67 ], [ %56, %64 ]
  tail call void @_ZdlPv(i8* nonnull %11) #23
  resume { i8*, i32 } %74

75:                                               ; preds = %49, %38
  %76 = bitcast %"union.std::_Any_data"* %0 to i8**
  store i8* %11, i8** %76, align 8, !tbaa !24
  br label %103

77:                                               ; preds = %3
  %78 = bitcast %"union.std::_Any_data"* %0 to %class.anon**
  %79 = load %class.anon*, %class.anon** %78, align 8, !tbaa !24
  %80 = icmp eq %class.anon* %79, null
  br i1 %80, label %103, label %81

81:                                               ; preds = %77
  %82 = getelementptr inbounds %class.anon, %class.anon* %79, i64 0, i32 1, i32 0, i32 1
  %83 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %82, align 8, !tbaa !20
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
  tail call void @__clang_call_terminate(i8* %90) #22
  unreachable

91:                                               ; preds = %85, %81
  %92 = getelementptr inbounds %class.anon, %class.anon* %79, i64 0, i32 0, i32 0, i32 1
  %93 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %92, align 8, !tbaa !20
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
  tail call void @__clang_call_terminate(i8* %100) #22
  unreachable

101:                                              ; preds = %95, %91
  %102 = bitcast %class.anon* %79 to i8*
  tail call void @_ZdlPv(i8* %102) #23
  br label %103

103:                                              ; preds = %101, %77, %75, %6, %4, %3
  ret i1 false
}

; Function Attrs: uwtable mustprogress
define internal void @"_ZNSt17_Function_handlerIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEZNS2_18BufferControlBlock18addRecycleCallbackEOSt8functionIS7_EE3$_0E9_M_invokeERKSt9_Any_dataOS4_OS6_"(%"union.std::_Any_data"* nocapture nonnull readonly align 8 dereferenceable(16) %0, %"class.NES::Runtime::detail::MemorySegment"** nocapture nonnull readonly align 8 dereferenceable(8) %1, %"class.NES::Runtime::BufferRecycler"** nocapture nonnull readonly align 8 dereferenceable(8) %2) #16 align 2 {
  %4 = alloca %"class.NES::Runtime::detail::MemorySegment"*, align 8
  %5 = alloca %"class.NES::Runtime::BufferRecycler"*, align 8
  %6 = alloca %"class.NES::Runtime::detail::MemorySegment"*, align 8
  %7 = alloca %"class.NES::Runtime::BufferRecycler"*, align 8
  %8 = bitcast %"union.std::_Any_data"* %0 to %class.anon**
  %9 = load %class.anon*, %class.anon** %8, align 8, !tbaa !24
  %10 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %1, align 8, !tbaa !24
  %11 = load %"class.NES::Runtime::BufferRecycler"*, %"class.NES::Runtime::BufferRecycler"** %2, align 8, !tbaa !24
  %12 = bitcast %"class.NES::Runtime::detail::MemorySegment"** %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %12)
  %13 = bitcast %"class.NES::Runtime::BufferRecycler"** %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %13)
  store %"class.NES::Runtime::detail::MemorySegment"* %10, %"class.NES::Runtime::detail::MemorySegment"** %6, align 8, !tbaa !24
  store %"class.NES::Runtime::BufferRecycler"* %11, %"class.NES::Runtime::BufferRecycler"** %7, align 8, !tbaa !24
  %14 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 1
  %15 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %14, align 8, !tbaa !20
  %16 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %15, null
  br i1 %16, label %17, label %18

17:                                               ; preds = %3
  tail call void @_ZSt25__throw_bad_function_callv() #20
  unreachable

18:                                               ; preds = %3
  %19 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 1
  %20 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %19, align 8, !tbaa !73
  %21 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 1, i32 0, i32 0
  call void %20(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %21, %"class.NES::Runtime::detail::MemorySegment"** nonnull align 8 dereferenceable(8) %6, %"class.NES::Runtime::BufferRecycler"** nonnull align 8 dereferenceable(8) %7)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %12)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %13)
  %22 = bitcast %"class.NES::Runtime::detail::MemorySegment"** %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %22)
  %23 = bitcast %"class.NES::Runtime::BufferRecycler"** %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %23)
  store %"class.NES::Runtime::detail::MemorySegment"* %10, %"class.NES::Runtime::detail::MemorySegment"** %4, align 8, !tbaa !24
  store %"class.NES::Runtime::BufferRecycler"* %11, %"class.NES::Runtime::BufferRecycler"** %5, align 8, !tbaa !24
  %24 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 1
  %25 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %24, align 8, !tbaa !20
  %26 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %25, null
  br i1 %26, label %27, label %28

27:                                               ; preds = %18
  call void @_ZSt25__throw_bad_function_callv() #20
  unreachable

28:                                               ; preds = %18
  %29 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 1
  %30 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %29, align 8, !tbaa !73
  %31 = getelementptr inbounds %class.anon, %class.anon* %9, i64 0, i32 0, i32 0, i32 0
  call void %30(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %31, %"class.NES::Runtime::detail::MemorySegment"** nonnull align 8 dereferenceable(8) %4, %"class.NES::Runtime::BufferRecycler"** nonnull align 8 dereferenceable(8) %5)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %22)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %23)
  ret void
}

; Function Attrs: noreturn
declare dso_local void @_ZSt25__throw_bad_function_callv() local_unnamed_addr #17

; Function Attrs: nofree norecurse nounwind uwtable willreturn mustprogress
define dso_local nonnull %"class.NES::Runtime::detail::BufferControlBlock"* @_ZN3NES7Runtime6detail18BufferControlBlock6retainEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull returned dereferenceable(88) %0) local_unnamed_addr #14 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %3 = atomicrmw add i32* %2, i32 1 seq_cst
  ret %"class.NES::Runtime::detail::BufferControlBlock"* %0
}

; Function Attrs: uwtable
define dso_local zeroext i1 @_ZN3NES7Runtime6detail18BufferControlBlock7releaseEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %0) local_unnamed_addr #1 align 2 personality i8* bitcast (i32 (...)* @__gxx_personality_v0 to i8*) {
  %2 = alloca %"class.NES::Runtime::detail::MemorySegment"*, align 8
  %3 = alloca %"class.NES::Runtime::BufferRecycler"*, align 8
  %4 = alloca %"class.std::__cxx11::basic_string", align 8
  %5 = alloca %"class.std::__cxx11::basic_stringbuf", align 8
  %6 = alloca %"class.std::basic_ostream", align 8
  %7 = alloca %"class.std::__cxx11::basic_string", align 8
  %8 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 0, i32 0, i32 0
  %9 = atomicrmw sub i32* %8, i32 1 seq_cst
  %10 = icmp eq i32 %9, 1
  br i1 %10, label %11, label %29

11:                                               ; preds = %1
  %12 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  store i32 0, i32* %12, align 4, !tbaa !71
  %13 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 6
  %14 = load %"class.NES::Runtime::detail::MemorySegment"*, %"class.NES::Runtime::detail::MemorySegment"** %13, align 8, !tbaa !12
  %15 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 7
  %16 = bitcast %"struct.std::atomic.0"* %15 to i64*
  %17 = load atomic i64, i64* %16 seq_cst, align 8
  %18 = inttoptr i64 %17 to %"class.NES::Runtime::BufferRecycler"*
  %19 = bitcast %"class.NES::Runtime::detail::MemorySegment"** %2 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %19)
  %20 = bitcast %"class.NES::Runtime::BufferRecycler"** %3 to i8*
  call void @llvm.lifetime.start.p0i8(i64 8, i8* nonnull %20)
  store %"class.NES::Runtime::detail::MemorySegment"* %14, %"class.NES::Runtime::detail::MemorySegment"** %2, align 8, !tbaa !24
  store %"class.NES::Runtime::BufferRecycler"* %18, %"class.NES::Runtime::BufferRecycler"** %3, align 8, !tbaa !24
  %21 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 1
  %22 = load i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)*, i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)** %21, align 8, !tbaa !20
  %23 = icmp eq i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* %22, null
  br i1 %23, label %24, label %25

24:                                               ; preds = %11
  tail call void @_ZSt25__throw_bad_function_callv() #20
  unreachable

25:                                               ; preds = %11
  %26 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 1
  %27 = load void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)*, void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)** %26, align 8, !tbaa !73
  %28 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 8, i32 0, i32 0
  call void %27(%"union.std::_Any_data"* nonnull align 8 dereferenceable(16) %28, %"class.NES::Runtime::detail::MemorySegment"** nonnull align 8 dereferenceable(8) %2, %"class.NES::Runtime::BufferRecycler"** nonnull align 8 dereferenceable(8) %3)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %19)
  call void @llvm.lifetime.end.p0i8(i64 8, i8* nonnull %20)
  br label %146

29:                                               ; preds = %1
  %30 = icmp eq i32 %9, 0
  br i1 %30, label %31, label %146

31:                                               ; preds = %29
  %32 = bitcast %"class.std::__cxx11::basic_string"* %4 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %32) #19
  call void @_ZN3NES25collectAndPrintStacktraceB5cxx11Ev(%"class.std::__cxx11::basic_string"* nonnull sret(%"class.std::__cxx11::basic_string") align 8 %4)
  %33 = bitcast %"class.std::__cxx11::basic_stringbuf"* %5 to i8*
  call void @llvm.lifetime.start.p0i8(i64 104, i8* nonnull %33) #19
  %34 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  %35 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 1
  %36 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 7
  %37 = bitcast i8** %35 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(48) %37, i8 0, i64 48, i1 false) #19
  call void @_ZNSt6localeC1Ev(%"class.std::locale"* nonnull dereferenceable(8) %36) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  %38 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 1
  store i32 24, i32* %38, align 8, !tbaa !27
  %39 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2
  %40 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 2
  %41 = bitcast %"class.std::__cxx11::basic_string"* %39 to %union.anon**
  store %union.anon* %40, %union.anon** %41, align 8, !tbaa !32
  %42 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 1
  store i64 0, i64* %42, align 8, !tbaa !33
  %43 = bitcast %union.anon* %40 to i8*
  store i8 0, i8* %43, align 8, !tbaa !23
  %44 = bitcast %"class.std::basic_ostream"* %6 to i8*
  call void @llvm.lifetime.start.p0i8(i64 272, i8* nonnull %44) #19
  %45 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0
  %46 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1
  %47 = getelementptr inbounds %"class.std::basic_ios", %"class.std::basic_ios"* %46, i64 0, i32 0
  call void @_ZNSt8ios_baseC2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %47) #19
  %48 = getelementptr %"class.std::basic_ios", %"class.std::basic_ios"* %46, i64 0, i32 0, i32 0
  %49 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 1
  store %"class.std::basic_ostream"* null, %"class.std::basic_ostream"** %49, align 8, !tbaa !34
  %50 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 2
  store i8 0, i8* %50, align 8, !tbaa !37
  %51 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 3
  store i8 0, i8* %51, align 1, !tbaa !38
  %52 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 4
  %53 = bitcast %"class.std::basic_streambuf"** %52 to i8*
  call void @llvm.memset.p0i8.i64(i8* nonnull align 8 dereferenceable(32) %53, i8 0, i64 32, i1 false) #19
  %54 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 0
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 3) to i32 (...)**), i32 (...)*** %54, align 8, !tbaa !25
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 1, i64 3) to i32 (...)**), i32 (...)*** %48, align 8, !tbaa !25
  %55 = load i64, i64* bitcast (i8** getelementptr inbounds ({ [5 x i8*], [5 x i8*] }, { [5 x i8*], [5 x i8*] }* @_ZTVSo, i64 0, inrange i32 0, i64 0) to i64*), align 8
  %56 = getelementptr inbounds i8, i8* %44, i64 %55
  %57 = bitcast i8* %56 to %"class.std::basic_ios"*
  invoke void @_ZNSt9basic_iosIcSt11char_traitsIcEE4initEPSt15basic_streambufIcS1_E(%"class.std::basic_ios"* nonnull dereferenceable(264) %57, %"class.std::basic_streambuf"* nonnull %45)
          to label %60 unwind label %58

58:                                               ; preds = %31
  %59 = landingpad { i8*, i32 }
          cleanup
  br label %131

60:                                               ; preds = %31
  %61 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %6, i8* nonnull getelementptr inbounds ([36 x i8], [36 x i8]* @.str.15, i64 0, i64 0), i64 35)
          to label %62 unwind label %118

62:                                               ; preds = %60
  %63 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %6, i8* nonnull getelementptr inbounds ([17 x i8], [17 x i8]* @.str.5, i64 0, i64 0), i64 16)
          to label %64 unwind label %118

64:                                               ; preds = %62
  %65 = invoke nonnull align 8 dereferenceable(8) %"class.std::basic_ostream"* @_ZSt16__ostream_insertIcSt11char_traitsIcEERSt13basic_ostreamIT_T0_ES6_PKS3_l(%"class.std::basic_ostream"* nonnull align 8 dereferenceable(8) %6, i8* nonnull getelementptr inbounds ([57 x i8], [57 x i8]* @.str.16, i64 0, i64 0), i64 56)
          to label %66 unwind label %118

66:                                               ; preds = %64
  %67 = bitcast %"class.std::__cxx11::basic_string"* %7 to i8*
  call void @llvm.lifetime.start.p0i8(i64 32, i8* nonnull %67) #19
  call void @llvm.experimental.noalias.scope.decl(metadata !83)
  %68 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 2
  %69 = bitcast %"class.std::__cxx11::basic_string"* %7 to %union.anon**
  store %union.anon* %68, %union.anon** %69, align 8, !tbaa !32, !alias.scope !83
  %70 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 1
  store i64 0, i64* %70, align 8, !tbaa !33, !alias.scope !83
  %71 = bitcast %union.anon* %68 to i8*
  store i8 0, i8* %71, align 8, !tbaa !23, !alias.scope !83
  %72 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 5
  %73 = load i8*, i8** %72, align 8, !tbaa !42, !noalias !83
  %74 = icmp eq i8* %73, null
  br i1 %74, label %97, label %75

75:                                               ; preds = %66
  %76 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 3
  %77 = load i8*, i8** %76, align 8, !tbaa !45, !noalias !83
  %78 = icmp ugt i8* %73, %77
  %79 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 0, i32 4
  %80 = load i8*, i8** %79, align 8, !tbaa !46
  br i1 %78, label %81, label %92

81:                                               ; preds = %75
  %82 = ptrtoint i8* %73 to i64
  %83 = ptrtoint i8* %80 to i64
  %84 = sub i64 %82, %83
  %85 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %7, i64 0, i64 0, i8* %80, i64 %84)
          to label %98 unwind label %86

86:                                               ; preds = %97, %92, %81
  %87 = landingpad { i8*, i32 }
          cleanup
  %88 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %89 = load i8*, i8** %88, align 8, !tbaa !47, !alias.scope !83
  %90 = icmp eq i8* %89, %71
  br i1 %90, label %126, label %91

91:                                               ; preds = %86
  call void @_ZdlPv(i8* %89) #19
  br label %126

92:                                               ; preds = %75
  %93 = ptrtoint i8* %77 to i64
  %94 = ptrtoint i8* %80 to i64
  %95 = sub i64 %93, %94
  %96 = invoke nonnull align 8 dereferenceable(32) %"class.std::__cxx11::basic_string"* @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE10_M_replaceEmmPKcm(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %7, i64 0, i64 0, i8* %80, i64 %95)
          to label %98 unwind label %86

97:                                               ; preds = %66
  invoke void @_ZNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE9_M_assignERKS4_(%"class.std::__cxx11::basic_string"* nonnull dereferenceable(32) %7, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %39)
          to label %98 unwind label %86

98:                                               ; preds = %97, %92, %81
  invoke void @_ZN3NES10Exceptions19invokeErrorHandlersERKNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEEOS6_(%"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %7, %"class.std::__cxx11::basic_string"* nonnull align 8 dereferenceable(32) %4)
          to label %99 unwind label %120

99:                                               ; preds = %98
  %100 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %101 = load i8*, i8** %100, align 8, !tbaa !47
  %102 = icmp eq i8* %101, %71
  br i1 %102, label %104, label %103

103:                                              ; preds = %99
  call void @_ZdlPv(i8* %101) #19
  br label %104

104:                                              ; preds = %103, %99
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %67) #19
  %105 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 0
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %105) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %44) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  %106 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 0, i32 0
  %107 = load i8*, i8** %106, align 8, !tbaa !47
  %108 = icmp eq i8* %107, %43
  br i1 %108, label %110, label %109

109:                                              ; preds = %104
  call void @_ZdlPv(i8* %107) #19
  br label %110

110:                                              ; preds = %109, %104
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %36) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %33) #19
  %111 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 0, i32 0
  %112 = load i8*, i8** %111, align 8, !tbaa !47
  %113 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 2
  %114 = bitcast %union.anon* %113 to i8*
  %115 = icmp eq i8* %112, %114
  br i1 %115, label %117, label %116

116:                                              ; preds = %110
  call void @_ZdlPv(i8* %112) #19
  br label %117

117:                                              ; preds = %116, %110
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %32) #19
  br label %146

118:                                              ; preds = %64, %62, %60
  %119 = landingpad { i8*, i32 }
          cleanup
  br label %128

120:                                              ; preds = %98
  %121 = landingpad { i8*, i32 }
          cleanup
  %122 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %7, i64 0, i32 0, i32 0
  %123 = load i8*, i8** %122, align 8, !tbaa !47
  %124 = icmp eq i8* %123, %71
  br i1 %124, label %126, label %125

125:                                              ; preds = %120
  call void @_ZdlPv(i8* %123) #19
  br label %126

126:                                              ; preds = %125, %120, %91, %86
  %127 = phi { i8*, i32 } [ %87, %91 ], [ %87, %86 ], [ %121, %120 ], [ %121, %125 ]
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %67) #19
  br label %128

128:                                              ; preds = %126, %118
  %129 = phi { i8*, i32 } [ %127, %126 ], [ %119, %118 ]
  %130 = getelementptr inbounds %"class.std::basic_ostream", %"class.std::basic_ostream"* %6, i64 0, i32 1, i32 0
  br label %131

131:                                              ; preds = %128, %58
  %132 = phi %"class.std::ios_base"* [ %47, %58 ], [ %130, %128 ]
  %133 = phi { i8*, i32 } [ %59, %58 ], [ %129, %128 ]
  call void @_ZNSt8ios_baseD2Ev(%"class.std::ios_base"* nonnull dereferenceable(216) %132) #19
  call void @llvm.lifetime.end.p0i8(i64 272, i8* nonnull %44) #19
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  %134 = getelementptr inbounds %"class.std::__cxx11::basic_stringbuf", %"class.std::__cxx11::basic_stringbuf"* %5, i64 0, i32 2, i32 0, i32 0
  %135 = load i8*, i8** %134, align 8, !tbaa !47
  %136 = icmp eq i8* %135, %43
  br i1 %136, label %138, label %137

137:                                              ; preds = %131
  call void @_ZdlPv(i8* %135) #19
  br label %138

138:                                              ; preds = %137, %131
  store i32 (...)** bitcast (i8** getelementptr inbounds ({ [16 x i8*] }, { [16 x i8*] }* @_ZTVSt15basic_streambufIcSt11char_traitsIcEE, i64 0, inrange i32 0, i64 2) to i32 (...)**), i32 (...)*** %34, align 8, !tbaa !25
  call void @_ZNSt6localeD1Ev(%"class.std::locale"* nonnull dereferenceable(8) %36) #19
  call void @llvm.lifetime.end.p0i8(i64 104, i8* nonnull %33) #19
  %139 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 0, i32 0
  %140 = load i8*, i8** %139, align 8, !tbaa !47
  %141 = getelementptr inbounds %"class.std::__cxx11::basic_string", %"class.std::__cxx11::basic_string"* %4, i64 0, i32 2
  %142 = bitcast %union.anon* %141 to i8*
  %143 = icmp eq i8* %140, %142
  br i1 %143, label %145, label %144

144:                                              ; preds = %138
  call void @_ZdlPv(i8* %140) #19
  br label %145

145:                                              ; preds = %144, %138
  call void @llvm.lifetime.end.p0i8(i64 32, i8* nonnull %32) #19
  resume { i8*, i32 } %133

146:                                              ; preds = %117, %29, %25
  ret i1 %10
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock17getNumberOfTuplesEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #15 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  %3 = load i32, i32* %2, align 4, !tbaa !71
  %4 = zext i32 %3 to i64
  ret i64 %4
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn writeonly mustprogress
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock17setNumberOfTuplesEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #18 align 2 {
  %3 = trunc i64 %1 to i32
  %4 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 1
  store i32 %3, i32* %4, align 4, !tbaa !71
  ret void
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock12getWatermarkEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #15 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  %3 = load i64, i64* %2, align 8, !tbaa !74
  ret i64 %3
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn writeonly mustprogress
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock12setWatermarkEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #18 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 2
  store i64 %1, i64* %3, align 8, !tbaa !74
  ret void
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock17getSequenceNumberEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #15 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 3
  %3 = load i64, i64* %2, align 8, !tbaa !86
  ret i64 %3
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn writeonly mustprogress
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock17setSequenceNumberEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #18 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 3
  store i64 %1, i64* %3, align 8, !tbaa !86
  ret void
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn writeonly mustprogress
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock20setCreationTimestampEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #18 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  store i64 %1, i64* %3, align 8, !tbaa !72
  ret void
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock20getCreationTimestampEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #15 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 4
  %3 = load i64, i64* %2, align 8, !tbaa !72
  ret i64 %3
}

; Function Attrs: norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i64 @_ZNK3NES7Runtime6detail18BufferControlBlock11getOriginIdEv(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull readonly dereferenceable(88) %0) local_unnamed_addr #15 align 2 {
  %2 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 5
  %3 = load i64, i64* %2, align 8, !tbaa !75
  ret i64 %3
}

; Function Attrs: nofree norecurse nounwind uwtable willreturn writeonly mustprogress
define dso_local void @_ZN3NES7Runtime6detail18BufferControlBlock11setOriginIdEm(%"class.NES::Runtime::detail::BufferControlBlock"* nocapture nonnull dereferenceable(88) %0, i64 %1) local_unnamed_addr #18 align 2 {
  %3 = getelementptr inbounds %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %0, i64 0, i32 5
  store i64 %1, i64* %3, align 8, !tbaa !75
  ret void
}

; Function Attrs: uwtable mustprogress
define dso_local void @_ZN3NES7Runtime6detail26zmqBufferRecyclingCallbackEPvS2_(i8* nocapture readnone %0, i8* %1) local_unnamed_addr #16 {
  %3 = bitcast i8* %1 to %"class.NES::Runtime::detail::BufferControlBlock"*
  %4 = tail call zeroext i1 @_ZN3NES7Runtime6detail18BufferControlBlock7releaseEv(%"class.NES::Runtime::detail::BufferControlBlock"* nonnull dereferenceable(88) %3)
  ret void
}

attributes #0 = { alwaysinline nounwind uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #3 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #4 = { nofree nounwind }
attributes #5 = { nofree nounwind uwtable willreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #6 = { argmemonly nofree nosync nounwind willreturn }
attributes #7 = { argmemonly nofree nosync nounwind willreturn writeonly }
attributes #8 = { inaccessiblememonly nofree nosync nounwind willreturn }
attributes #9 = { nobuiltin nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #10 = { nounwind uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #11 = { nobuiltin nofree allocsize(0) "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #12 = { nofree nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #13 = { noinline noreturn nounwind }
attributes #14 = { nofree norecurse nounwind uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #15 = { norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #16 = { uwtable mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #17 = { noreturn "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #18 = { nofree norecurse nounwind uwtable willreturn writeonly mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #19 = { nounwind }
attributes #20 = { noreturn }
attributes #21 = { builtin allocsize(0) }
attributes #22 = { noreturn nounwind }
attributes #23 = { builtin nounwind }

!llvm.ident = !{!0, !0}
!llvm.module.flags = !{!1}

!0 = !{!"Ubuntu clang version 12.0.0-3ubuntu1~20.04.5"}
!1 = !{i32 1, !"wchar_size", i32 4}
!2 = !{!3, !4, i64 0}
!3 = !{!"_ZTSN3NES7Runtime11TupleBufferE", !4, i64 0, !4, i64 8, !7, i64 16}
!4 = !{!"any pointer", !5, i64 0}
!5 = !{!"omnipotent char", !6, i64 0}
!6 = !{!"Simple C++ TBAA"}
!7 = !{!"int", !5, i64 0}
!8 = !{!9, !4, i64 0}
!9 = !{!"_ZTSN3NES7Runtime6detail13MemorySegmentE", !4, i64 0, !7, i64 8, !4, i64 16}
!10 = !{!9, !7, i64 8}
!11 = !{!9, !4, i64 16}
!12 = !{!13, !4, i64 40}
!13 = !{!"_ZTSN3NES7Runtime6detail18BufferControlBlockE", !14, i64 0, !7, i64 4, !15, i64 8, !15, i64 16, !15, i64 24, !15, i64 32, !4, i64 40, !16, i64 48, !18, i64 56}
!14 = !{!"_ZTSSt6atomicIiE"}
!15 = !{!"long", !5, i64 0}
!16 = !{!"_ZTSSt6atomicIPN3NES7Runtime14BufferRecyclerEE", !17, i64 0}
!17 = !{!"_ZTSSt13__atomic_baseIPN3NES7Runtime14BufferRecyclerEE", !4, i64 0}
!18 = !{!"_ZTSSt8functionIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEE", !4, i64 24}
!19 = !{!17, !4, i64 0}
!20 = !{!21, !4, i64 16}
!21 = !{!"_ZTSSt14_Function_base", !5, i64 0, !4, i64 16}
!22 = !{i64 0, i64 8, !23, i64 0, i64 8, !23, i64 0, i64 8, !23, i64 0, i64 16, !23, i64 0, i64 16, !23}
!23 = !{!5, !5, i64 0}
!24 = !{!4, !4, i64 0}
!25 = !{!26, !26, i64 0}
!26 = !{!"vtable pointer", !6, i64 0}
!27 = !{!28, !29, i64 64}
!28 = !{!"_ZTSNSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEEE", !29, i64 64, !30, i64 72}
!29 = !{!"_ZTSSt13_Ios_Openmode", !5, i64 0}
!30 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEEE", !31, i64 0, !15, i64 8, !5, i64 16}
!31 = !{!"_ZTSNSt7__cxx1112basic_stringIcSt11char_traitsIcESaIcEE12_Alloc_hiderE", !4, i64 0}
!32 = !{!31, !4, i64 0}
!33 = !{!30, !15, i64 8}
!34 = !{!35, !4, i64 216}
!35 = !{!"_ZTSSt9basic_iosIcSt11char_traitsIcEE", !4, i64 216, !5, i64 224, !36, i64 225, !4, i64 232, !4, i64 240, !4, i64 248, !4, i64 256}
!36 = !{!"bool", !5, i64 0}
!37 = !{!35, !5, i64 224}
!38 = !{!35, !36, i64 225}
!39 = !{!40}
!40 = distinct !{!40, !41, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!41 = distinct !{!41, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!42 = !{!43, !4, i64 40}
!43 = !{!"_ZTSSt15basic_streambufIcSt11char_traitsIcEE", !4, i64 8, !4, i64 16, !4, i64 24, !4, i64 32, !4, i64 40, !4, i64 48, !44, i64 56}
!44 = !{!"_ZTSSt6locale", !4, i64 0}
!45 = !{!43, !4, i64 24}
!46 = !{!43, !4, i64 32}
!47 = !{!30, !4, i64 0}
!48 = !{!49, !4, i64 0}
!49 = !{!"_ZTSNSt12experimental15fundamentals_v215source_locationE", !4, i64 0, !4, i64 8, !7, i64 16, !7, i64 20}
!50 = !{!51}
!51 = distinct !{!51, !52, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii: argument 0"}
!52 = distinct !{!52, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii"}
!53 = !{!49, !4, i64 8}
!54 = !{!49, !7, i64 16}
!55 = !{!49, !7, i64 20}
!56 = !{!57}
!57 = distinct !{!57, !58, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!58 = distinct !{!58, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!59 = !{!60}
!60 = distinct !{!60, !61, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii: argument 0"}
!61 = distinct !{!61, !"_ZNSt12experimental15fundamentals_v215source_location7currentEPKcS3_ii"}
!62 = !{!63}
!63 = distinct !{!63, !64, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!64 = distinct !{!64, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!65 = !{!66}
!66 = distinct !{!66, !67, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!67 = distinct !{!67, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!68 = !{!69}
!69 = distinct !{!69, !70, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!70 = distinct !{!70, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!71 = !{!13, !7, i64 4}
!72 = !{!13, !15, i64 24}
!73 = !{!18, !4, i64 24}
!74 = !{!13, !15, i64 8}
!75 = !{!13, !15, i64 32}
!76 = !{!15, !15, i64 0}
!77 = !{!78}
!78 = distinct !{!78, !79, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!79 = distinct !{!79, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!80 = !{!81}
!81 = distinct !{!81, !82, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!82 = distinct !{!82, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!83 = !{!84}
!84 = distinct !{!84, !85, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv: argument 0"}
!85 = distinct !{!85, !"_ZNKSt7__cxx1115basic_stringbufIcSt11char_traitsIcESaIcEE3strEv"}
!86 = !{!13, !15, i64 16}
