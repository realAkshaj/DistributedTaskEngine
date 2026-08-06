#pragma once

#include <chrono>
#include <cstdarg>
#include <cstdio>
#include <ctime>
#include <mutex>

namespace dte {

inline std::mutex& log_mutex() {
  static std::mutex m;
  return m;
}

inline void log_line(const char* level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

inline void log_line(const char* level, const char* fmt, ...) {
  auto now = std::chrono::system_clock::now();
  auto t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count() % 1000;
  std::tm tm{};
  gmtime_r(&t, &tm);

  std::lock_guard<std::mutex> lk(log_mutex());
  std::fprintf(stderr, "%04d-%02d-%02dT%02d:%02d:%02d.%03lldZ %s ",
               tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday,
               tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<long long>(ms), level);
  va_list args;
  va_start(args, fmt);
  std::vfprintf(stderr, fmt, args);
  va_end(args);
  std::fputc('\n', stderr);
}

}  // namespace dte

#define LOG_INFO(...) ::dte::log_line("INFO ", __VA_ARGS__)
#define LOG_WARN(...) ::dte::log_line("WARN ", __VA_ARGS__)
#define LOG_ERROR(...) ::dte::log_line("ERROR", __VA_ARGS__)
