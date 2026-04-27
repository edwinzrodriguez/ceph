#include <cxxabi.h> // for __cxa_demangle
#include <dlfcn.h>
#include <pthread.h>
#include <unistd.h>

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <string>
#if HAVE_LIBBACKTRACE
#include <backtrace.h>
#else
#include <execinfo.h>
#endif

static __thread int depth = 0;
static FILE* trace_file = nullptr;
static char trace_file_buffer[64 * 1024];
static __thread bool in_tracer = false;

// ==================== SAFE SYMBOL CACHE ====================
#define MAX_SYMBOLS 16384

struct SymbolEntry {
  void* addr;
  char name[256];
};

static SymbolEntry symbol_table[MAX_SYMBOLS] = {}; // zero-initialized
static pthread_mutex_t symbol_mutex = PTHREAD_MUTEX_INITIALIZER;
static size_t symbol_table_count = 0;
static size_t symbol_insert_fails = 0;

__attribute__((no_instrument_function)) static inline uint32_t
hash_addr(void* addr)
{
  auto v = (uintptr_t)addr;
  v ^= v >> 16;
  v *= 0x85ebca6b;
  v ^= v >> 13;
  v *= 0xc2b2ae35;
  v ^= v >> 16;
  return (uint32_t)v % MAX_SYMBOLS;
}

__attribute__((no_instrument_function)) static SymbolEntry*
lookup_symbol(void* addr)
{
  uint32_t start_idx = hash_addr(addr);
  for (uint32_t i = 0; i < MAX_SYMBOLS; ++i) {
    uint32_t probe = (start_idx + i) % MAX_SYMBOLS;
    if (symbol_table[probe].addr == addr ||
        symbol_table[probe].addr == nullptr) {
      return &symbol_table[probe];
    }
  }
  symbol_insert_fails++;
  return nullptr;
}

// === CRITICAL: No instrumentation on this function ===
__attribute__((no_instrument_function)) static void
fill_symbol_entry(SymbolEntry* entry, void* addr, const char* name)
{
  if (!entry) {
    return;
  }
  strncpy(entry->name, name, sizeof(entry->name) - 1);
  entry->name[sizeof(entry->name) - 1] = '\0';
  entry->addr = addr;
  symbol_table_count++;
}

__attribute__((no_instrument_function)) static uintptr_t
get_library_base(const char* libname)
{
  FILE* maps = fopen("/proc/self/maps", "r");
  if (!maps) {
    return 0;
  }

  char line[512];
  uintptr_t base = 0;

  while (fgets(line, sizeof(line), maps)) {
    if (strstr(line, libname)) {
      sscanf(line, "%lx", &base);
      break;
    }
  }
  fclose(maps);
  return base;
}

// ==================== LIBRARY BASE + OFFSET HELPER ====================
// Returns something like "(/usr/local/lib64/libcephfs.so+0x1f10)"
// which is perfect for addr2line post-processing
__attribute__((no_instrument_function)) static const char*
get_address_string(void* addr)
{
  static char result[512];

  Dl_info info = {};
  if (!dladdr(addr, &info) || !info.dli_fname) {
    snprintf(result, sizeof(result), "0x%lx", (unsigned long)addr);
    return result;
  }

  // Get load base of this library
  uintptr_t base = get_library_base(info.dli_fname);

  uintptr_t offset = (uintptr_t)addr - base;
  snprintf(result, sizeof(result), "(%s+0x%lx)", info.dli_fname, offset);
  return result;
}

// ==================== SYMBOL RESOLUTION ====================
#if HAVE_LIBBACKTRACE
static struct backtrace_state* bt_state = nullptr;
static size_t init_symbol_resolver_calls = 0;

// One-time initialization (call from main())
__attribute__((no_instrument_function)) void
init_symbol_resolver(const char* progname)
{
  if (!bt_state) {
    bt_state = backtrace_create_state(
        progname,
        1, // verbose = 1 for more debug
        [](void* data, const char* msg, int errnum) {
          fprintf(stderr, "libbacktrace error: %s (%d)\n", msg, errnum);
        },
        nullptr);
    init_symbol_resolver_calls++;
  }
}

// ==================== DLOPEN HOOK - re-init libbacktrace ====================
typedef void* (*real_dlopen_t)(const char* filename, int flag);
static real_dlopen_t real_dlopen = nullptr;

extern "C" __attribute__((visibility("default"), no_instrument_function)) void*
dlopen(const char* filename, int flag)
{
  if (!real_dlopen) {
    real_dlopen = (real_dlopen_t)dlsym(RTLD_NEXT, "dlopen");
  }

  void* handle = real_dlopen(filename, flag);

  // Re-initialize libbacktrace so it sees newly dlopen'ed libraries (FSALs)
  if (handle && filename && bt_state) {
    bt_state = backtrace_create_state(
        filename, 1,
        [](void* data, const char* msg, int errnum) {
          fprintf(stderr, "libbacktrace re-init error: %s (%d)\n", msg, errnum);
        },
        nullptr);
  }
  return handle;
}

// Callback for backtrace_pcinfo (gives filename + line + function)
__attribute__((no_instrument_function)) static int
symbol_callback(
    void* data,
    uintptr_t pc,
    const char* filename,
    int lineno,
    const char* function)
{
  char** result = (char**)data;
  if (function) {
    int status = 0;
    char* demangled = abi::__cxa_demangle(function, nullptr, nullptr, &status);
    if (demangled && status == 0) {
      *result = strdup(demangled);
      free(demangled);
    } else {
      *result = strdup(function);
    }
  } else if (filename) {
    static char buf[256];
    snprintf(buf, sizeof(buf), "%s:%d", filename, lineno);
    *result = strdup(buf);
  }
  return 0; // continue (but we only need one)
}

// Fallback error callback
__attribute__((no_instrument_function)) static void
error_callback(void* data, const char* msg, int errnum)
{
  // silent or log
}

__attribute__((no_instrument_function)) static const char*
resolve_symbol_libbacktrace(SymbolEntry* entry, void* addr)
{
  if (!bt_state || !entry) {
    return nullptr;
  }

  char* sym = nullptr;

  // Primary: full PC info (best for inlined / template functions)
  backtrace_pcinfo(
      bt_state, (uintptr_t)addr, symbol_callback, error_callback, &sym);

  // Secondary: symbol-only
  if (!sym) {
    // Fallback to symbol-only (no line info)
    backtrace_syminfo(
        bt_state, (uintptr_t)addr,
        [](void* data, uintptr_t pc, const char* symname, uintptr_t symval,
           uintptr_t symsize) {
          char** r = (char**)data;
          if (symname) {
            int status = 0;
            char* dem = abi::__cxa_demangle(symname, nullptr, nullptr, &status);
            *r = (dem && status == 0) ? strdup(dem) : strdup(symname);
            if (dem)
              free(dem);
          }
        },
        error_callback, &sym);
  }

  if (sym) {
    fill_symbol_entry(entry, addr, sym);
    free(sym);
    return entry->name;
  }
  return nullptr;
}

#endif


// === CRITICAL: No instrumentation on this function ===
__attribute__((no_instrument_function)) static const char*
resolve_symbol_dladdr(SymbolEntry* entry, void* addr)
{
  Dl_info info = {nullptr};
  if (dladdr(addr, &info) && info.dli_sname) {
    int status = 0;
    char* demangled =
        abi::__cxa_demangle(info.dli_sname, nullptr, nullptr, &status);
    if (demangled && status == 0) {
      fill_symbol_entry(entry, addr, demangled);
      free(demangled);
    } else {
      fill_symbol_entry(entry, addr, info.dli_sname);
    }
    return entry->name;
  }
  return nullptr;
}

#ifndef HAVE_LIBBACKTRACE
__attribute__((no_instrument_function)) static const char*
resolve_symbol_backtrace(SymbolEntry* entry, void* addr)
{
  void* dummy_buffer[2] = {nullptr, addr}; // fake a 2-frame backtrace
  char** symbols = backtrace_symbols(dummy_buffer, 2);
  const char* result = nullptr;

  if (symbols && symbols[1]) {
    // symbols[1] typically looks like: "program[0x402832] (now+0x12) [0x402832]"
    const char* sym = symbols[1];

    // Simple parsing: extract everything between first '(' and last ')'
    const char* start = strchr(sym, '(');
    if (start) {
      start++; // skip '('
      const char* end = strrchr(start, ')');
      size_t len = end ? (size_t)(end - start) : strlen(start);

      char temp_name[256];
      size_t to_copy = (len < sizeof(temp_name)) ? len : sizeof(temp_name) - 1;
      memcpy(temp_name, start, to_copy);
      temp_name[to_copy] = '\0';

      fill_symbol_entry(entry, addr, temp_name);
      result = entry->name;
    }
    free(symbols);
  }
  return result;
}
#endif

// === CRITICAL: No instrumentation on this function ===
__attribute__((no_instrument_function)) static const char*
get_symbol_name(void* addr, void* call_site)
{
  if (!addr) {
    return "nullptr";
  }

  pthread_mutex_lock(&symbol_mutex);

  // Fast path: check cache first
  SymbolEntry* entry = lookup_symbol(addr);
  if (entry && entry->addr == addr) {
    pthread_mutex_unlock(&symbol_mutex);
    return entry->name;
  }

  if (!entry) {
    // Table full
    pthread_mutex_unlock(&symbol_mutex);
    static char fallback_buf[32];
    snprintf(fallback_buf, sizeof(fallback_buf), "0x%lx", (unsigned long)addr);
    return fallback_buf;
  }

  // New symbol, entry is an empty slot
  const char* result = resolve_symbol_dladdr(entry, addr);
#if HAVE_LIBBACKTRACE
  if (!result) {
    result = resolve_symbol_libbacktrace(entry, addr);
  }
#else
  if (!result) {
    result = resolve_symbol_backtrace(entry, addr);
  }
#endif

  // FINAL FALLBACK: always give something addr2line can use
  // After normal symbol resolution fails
  if (!result) {
    Dl_info info = {};
    if (dladdr(addr, &info) && info.dli_fname) {
      const char* addr_str =
          get_address_string(addr); // fallback to (lib.so+0x...)
      if (addr_str) {
        fill_symbol_entry(entry, addr, addr_str);
        result = entry->name;
      }
    }
  }

  if (!result && call_site && call_site != addr) {
    result = resolve_symbol_dladdr(entry, call_site);
    if (result) {
      // Associate the resolved name with the original addr as well
      entry->addr = addr;
    }
  }

  if (!result) {
    // Last resort: just the address as hex
    char addr_buf[32];
    snprintf(addr_buf, sizeof(addr_buf), "0x%lx", (unsigned long)addr);
    fill_symbol_entry(entry, addr, addr_buf);
    result = entry->name;
  }

  pthread_mutex_unlock(&symbol_mutex);
  return result;
}

// ==================== INIT / FINI ====================
__attribute__((constructor, no_instrument_function)) static void
init_tracer()
{
  const char* whitelist = getenv("TRACEFLOW_WHITELIST");
  if (whitelist) {
    bool found = false;
    const char* p = whitelist;
    size_t name_len = strlen(program_invocation_short_name);
    while (*p) {
      const char* next = strchr(p, ',');
      size_t len = next ? (size_t)(next - p) : strlen(p);
      if (len == name_len &&
          strncmp(p, program_invocation_short_name, len) == 0) {
        found = true;
        break;
      }
      if (!next)
        break;
      p = next + 1;
    }
    if (!found) {
      return;
    }
  } else {
    return;
  }
#if HAVE_LIBBACKTRACE
  init_symbol_resolver(program_invocation_name);
#endif

  const char* env_path = getenv("TRACEFLOW_FILE");
  char path_buf[256];
  const char* path = env_path;

  if (!path) {
    snprintf(
        path_buf, sizeof(path_buf), "/tmp/flow-trace.%s.%d.log",
        program_invocation_short_name, getpid());
    path = path_buf;
  }

  trace_file = fopen(path, "w");
  if (!trace_file) {
    perror("fopen trace.log");
    return;
  }
  setvbuf(trace_file, trace_file_buffer, _IOFBF, sizeof(trace_file_buffer));
  fprintf(
      trace_file, "%-16s %-16s %-15s %-6s %-4s %-18s %s\n", "timestamp_ns",
      "tid", "tname", "depth", "dir", "addr", "name");
  fprintf(
      trace_file,
      "------------------------------------------------------------------------"
      "---------------------------------------------\n");
  fflush(trace_file);
}

__attribute__((destructor, no_instrument_function)) static void
fini_tracer()
{
  if (trace_file) {
    FILE* tmp_trace_file = trace_file;
    trace_file = nullptr;
    fclose(tmp_trace_file);
  }
}

extern "C" {
void __cyg_profile_func_enter(void* this_fn, void* call_site)
    __attribute__((no_instrument_function));
void __cyg_profile_func_exit(void* this_fn, void* call_site)
    __attribute__((no_instrument_function));

void
__cyg_profile_func_enter(void* this_fn, void* call_site)
{
  if (trace_file == nullptr || in_tracer) {
    return;
  }
  in_tracer = true;
  // Optional: filter here (example: only *Client* or fsal/ceph functions)
  const char* name = get_symbol_name(this_fn, call_site);

  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  long long ns = ts.tv_sec * 1000000000LL + ts.tv_nsec;

  const pthread_t tid = pthread_self();
  char tname[16] = "unknown";
  pthread_getname_np(tid, tname, sizeof(tname));

  depth++;
  fprintf(
      trace_file, "%-16lld %-16lu %-15s %-6d %-4s %-18p %s\n", ns,
      static_cast<unsigned long>(tid), tname, depth, "->", this_fn, name);
  // fflush(trace_file);   // remove for lower overhead
  in_tracer = false;
}

void
__cyg_profile_func_exit(void* this_fn, void* call_site)
{
  if (trace_file == nullptr || in_tracer) {
    return;
  }
  in_tracer = true;

  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  long long ns = ts.tv_sec * 1000000000LL + ts.tv_nsec;

  pthread_t tid = pthread_self();
  char tname[16] = "unknown";
  pthread_getname_np(tid, tname, sizeof(tname));

  const char* name = get_symbol_name(this_fn, call_site);

  fprintf(
      trace_file, "%-16lld %-16lu %-15s %-6d %-4s %-18p %s\n", ns,
      static_cast<unsigned long>(tid), tname, depth, "<-", this_fn, name);

  depth--;
  if (depth < 0) {
    depth = 0;
  }
  // fflush(trace_file);
  in_tracer = false;
}
}
