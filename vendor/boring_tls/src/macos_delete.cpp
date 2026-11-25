// libc++'s operator delete, which a macOS executable links statically, as libc++ defines it (src/new.cpp) but hidden
// (build.zig compiles this with -fvisibility-global-new-delete=force-hidden). libc++ makes it weak and default-visibility
// so another image can replace it; an executable linking it would then export a weak definition, and dyld binds weak
// definitions afresh on every launch. Nothing links against the executable, so nothing could replace these.
#include <cstddef>
#include <cstdlib>

void operator delete(void *ptr) noexcept { std::free(ptr); }
void operator delete(void *ptr, std::size_t) noexcept { std::free(ptr); }
