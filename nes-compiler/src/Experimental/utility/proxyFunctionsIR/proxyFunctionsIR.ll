; ModuleID = 'proxyFunctionsIR.ll'
source_filename = "llvm-link"
target datalayout = "e-m:e-p270:32:32-p271:32:32-p272:64:64-i64:64-f80:128-n8:16:32:64-S128"
target triple = "x86_64-pc-linux-gnu"

%"class.NES::Runtime::detail::BufferControlBlock" = type { %"struct.std::atomic", i32, i64, i64, i64, i64, %"class.NES::Runtime::detail::MemorySegment"*, %"struct.std::atomic.0", %"class.std::function" }
%"struct.std::atomic" = type { %"struct.std::__atomic_base" }
%"struct.std::__atomic_base" = type { i32 }
%"class.NES::Runtime::detail::MemorySegment" = type { i8*, i32, %"class.NES::Runtime::detail::BufferControlBlock"* }
%"struct.std::atomic.0" = type { %"struct.std::__atomic_base.1" }
%"struct.std::__atomic_base.1" = type { %"class.NES::Runtime::BufferRecycler"* }
%"class.NES::Runtime::BufferRecycler" = type opaque
%"class.std::function" = type { %"class.std::_Function_base", void (%"union.std::_Any_data"*, %"class.NES::Runtime::detail::MemorySegment"**, %"class.NES::Runtime::BufferRecycler"**)* }
%"class.std::_Function_base" = type { %"union.std::_Any_data", i1 (%"union.std::_Any_data"*, %"union.std::_Any_data"*, i32)* }
%"union.std::_Any_data" = type { %"union.std::_Nocopy_types" }
%"union.std::_Nocopy_types" = type { { i64, i64 } }

; Function Attrs: alwaysinline norecurse nounwind readonly uwtable willreturn mustprogress
define dso_local i64 @getNumTuples(i8* nocapture readonly %0) local_unnamed_addr #4 {
  %2 = bitcast i8* %0 to %"class.NES::Runtime::detail::BufferControlBlock"**
  %3 = load %"class.NES::Runtime::detail::BufferControlBlock"*, %"class.NES::Runtime::detail::BufferControlBlock"** %2, align 8, !tbaa !2
  %.idx = getelementptr %"class.NES::Runtime::detail::BufferControlBlock", %"class.NES::Runtime::detail::BufferControlBlock"* %3, i64 0, i32 1
  %.idx.val = load i32, i32* %.idx, align 4, !tbaa !8
  %4 = zext i32 %.idx.val to i64
  ret i64 %4
}

attributes #0 = { uwtable "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #1 = { "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #2 = { nounwind "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "no-infs-fp-math"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #3 = { nofree nounwind }
attributes #4 = { alwaysinline norecurse nounwind readonly uwtable willreturn mustprogress "disable-tail-calls"="false" "frame-pointer"="none" "less-precise-fpmad"="false" "min-legal-vector-width"="0" "no-infs-fp-math"="false" "no-jump-tables"="false" "no-nans-fp-math"="false" "no-signed-zeros-fp-math"="false" "no-trapping-math"="true" "stack-protector-buffer-size"="8" "target-cpu"="x86-64" "target-features"="+cx8,+fxsr,+mmx,+sse,+sse2,+x87" "tune-cpu"="generic" "unsafe-fp-math"="false" "use-soft-float"="false" }
attributes #5 = { nounwind }

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
!8 = !{!9, !7, i64 4}
!9 = !{!"_ZTSN3NES7Runtime6detail18BufferControlBlockE", !10, i64 0, !7, i64 4, !11, i64 8, !11, i64 16, !11, i64 24, !11, i64 32, !4, i64 40, !12, i64 48, !14, i64 56}
!10 = !{!"_ZTSSt6atomicIiE"}
!11 = !{!"long", !5, i64 0}
!12 = !{!"_ZTSSt6atomicIPN3NES7Runtime14BufferRecyclerEE", !13, i64 0}
!13 = !{!"_ZTSSt13__atomic_baseIPN3NES7Runtime14BufferRecyclerEE", !4, i64 0}
!14 = !{!"_ZTSSt8functionIFvPN3NES7Runtime6detail13MemorySegmentEPNS1_14BufferRecyclerEEE", !4, i64 24}