// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//    https://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef NES_TIDY_MULTI_RETURN_VAL_CHECK_HPP
#define NES_TIDY_MULTI_RETURN_VAL_CHECK_HPP

#include "clang-tidy/ClangTidyCheck.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallPtrSet.h"

namespace clang::tidy::nes {

/// Warns on functions that return nautilus::val<T> (or a type that transitively
/// contains one) and have multiple return statements. Multiple returns in
/// nautilus-traced functions can cause issues with the tracing framework.
class MultiReturnValCheck : public ClangTidyCheck {
public:
    MultiReturnValCheck(StringRef Name, ClangTidyContext *Context)
        : ClangTidyCheck(Name, Context) {}

    void registerMatchers(ast_matchers::MatchFinder *Finder) override;
    void check(const ast_matchers::MatchFinder::MatchResult &Result) override;

    bool isLanguageVersionSupported(const LangOptions &LangOpts) const override {
        return LangOpts.CPlusPlus;
    }

private:
    /// Check if a QualType is directly nautilus::val<T>.
    static bool isNautilusValType(QualType Type);

    /// Check if a QualType is or transitively contains nautilus::val<T>.
    /// Inspects fields, base classes, and template arguments recursively.
    bool containsValType(QualType Type,
                         llvm::SmallPtrSetImpl<const RecordDecl *> &Visited);

    /// Cache for containsValType results, keyed by RecordDecl.
    llvm::DenseMap<const RecordDecl *, bool> ContainsValCache;

    /// Tracks source locations where fix-its have already been emitted (per TU),
    /// preventing duplicate fixes for the same function (e.g. template instantiations).
    llvm::DenseSet<SourceLocation> FixedLocations;
};

} // namespace clang::tidy::nes

#endif // NES_TIDY_MULTI_RETURN_VAL_CHECK_HPP
