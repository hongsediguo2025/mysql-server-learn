/* Copyright (c) 2026, Oracle and/or its affiliates.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0. */
#include "sql/preserve_trx_temp_gc.h"

#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <mutex>
#include <new>

#include "scope_guard.h"
#include "sql/preserve_trx_resource.h"
#include "sql/preserve_trx_transfer.h"

namespace {
constexpr char kPrivate[] = ".temp_receiver";
constexpr char kOwnerSuffix[] = ".owner";
constexpr char kOwnerMagic[] = "preserve-trx-temp-receiver-v1\n";
constexpr size_t kDepth = 8;
constexpr size_t kName = 256;
std::mutex directory_mutex;
std::string cached_parent, cached_directory;
std::atomic<ulonglong> scanned{0}, removed{0}, errors{0}, passes{0};

std::string normalized_root(std::string root) {
  while (root.size() > 1 && root.back() == '/') root.pop_back();
  return root;
}

bool boot_name(const char *name) {
  if (std::strlen(name) != 32) return false;
  for (size_t i = 0; i < 32; ++i) {
    if (!((name[i] >= '0' && name[i] <= '9') ||
          (name[i] >= 'a' && name[i] <= 'f')))
      return false;
  }
  return true;
}

int open_directory(int parent, const char *name) {
  return openat(parent, name, O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC);
}

bool ensure_directory(int parent, const char *name) {
  return mkdirat(parent, name, 0700) == 0 || errno == EEXIST;
}

// The marker is a sibling of the boot directory. Remove it only after rmdir:
// a crash during deletion cannot strand an unmarked, partly deleted tree.
void owner_name(const char *boot, char *name) {
  std::memcpy(name, boot, 32);
  std::memcpy(name + 32, kOwnerSuffix, sizeof(kOwnerSuffix));
}

size_t owner_contents(const char *boot, char *contents) {
  constexpr size_t prefix = sizeof(kOwnerMagic) - 1;
  std::memcpy(contents, kOwnerMagic, prefix);
  std::memcpy(contents + prefix, boot, 32);
  contents[prefix + 32] = '\n';
  return prefix + 33;
}

bool owned_boot(int directory, const char *boot) {
  char name[40], expected[96], actual[96];
  owner_name(boot, name);
  const size_t length = owner_contents(boot, expected);
  struct stat info;
  if (fstatat(directory, name, &info, AT_SYMLINK_NOFOLLOW) != 0 ||
      !S_ISREG(info.st_mode))
    return false;
  const int fd =
      openat(directory, name, O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC);
  if (fd < 0) return false;
  const auto close_fd = create_scope_guard([&] { close(fd); });
  if (fstat(fd, &info) != 0 || !S_ISREG(info.st_mode) ||
      info.st_size != static_cast<off_t>(length))
    return false;
  return pread(fd, actual, length, 0) == static_cast<ssize_t>(length) &&
         std::memcmp(actual, expected, length) == 0;
}

bool create_owner(int directory, const char *boot) {
  char name[40], contents[96];
  owner_name(boot, name);
  const size_t length = owner_contents(boot, contents);
  const int fd =
      openat(directory, name,
             O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC, 0600);
  if (fd < 0) return errno == EEXIST && owned_boot(directory, boot);
  const auto close_fd = create_scope_guard([&] { close(fd); });
  if (write(fd, contents, length) != static_cast<ssize_t>(length) ||
      fsync(fd) != 0) {
    unlinkat(directory, name, 0);
    return false;
  }
  return true;
}

struct Frame {
  DIR *cursor{nullptr};
  char name[kName]{};
  bool failed{false};
};
struct Scanner {
  std::string root, boot;
  Preserve_file_resource_lease files;
  Preserve_memory_lease memory;
  std::array<Frame, kDepth> frames;
  size_t depth{0};
  std::chrono::steady_clock::time_point next_pass{};

  void close() {
    while (depth != 0) {
      closedir(frames[--depth].cursor);
      frames[depth] = Frame{};
    }
    files.release();
    memory.release();
  }

  bool push(int fd, const char *name) {
    DIR *cursor = fdopendir(fd);
    if (cursor == nullptr) {
      ::close(fd);
      return false;
    }
    Frame &frame = frames[depth++];
    frame = Frame{};
    frame.cursor = cursor;
    std::strncpy(frame.name, name, kName - 1);
    return true;
  }

  bool open() {
    if (root.empty() || boot.empty() ||
        std::chrono::steady_clock::now() < next_pass)
      return false;
    // Reserve cursors plus one marker and one transient parent FD. Existing
    // disk occupancy remains in statvfs, not pending-write reservations.
    files = preserve_trx_acquire_file_resource_lease(root, kDepth + 2, 0);
    memory = preserve_trx_acquire_memory_lease(
        "temp-receiver-gc", Preserve_trx_memory_kind::TEMP_METADATA_IMPORT,
        kDepth * (sizeof(Frame) + 65536));
    if (!files.acquired() || !memory.acquired()) {
      close();
      return false;
    }
    const int parent = open_directory(AT_FDCWD, root.c_str());
    const int fd = parent < 0 ? -1 : open_directory(parent, kPrivate);
    const int saved_errno = errno;
    if (parent >= 0) ::close(parent);
    if (fd < 0 || !push(fd, "")) {
      if (fd >= 0 || saved_errno != ENOENT) ++errors;
      close();
      next_pass = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      return false;
    }
    return true;
  }

  void error() {
    ++errors;
    frames[depth - 1].failed = true;
  }

  void finish_directory() {
    Frame &child = frames[depth - 1];
    if (depth == 1) {
      ++passes;
      close();
      next_pass = std::chrono::steady_clock::now() + std::chrono::seconds(5);
      return;
    }
    const int parent = dirfd(frames[depth - 2].cursor);
    closedir(child.cursor);
    child.cursor = nullptr;
    if (!child.failed) {
      if (unlinkat(parent, child.name, AT_REMOVEDIR) == 0 || errno == ENOENT) {
        ++removed;
        if (depth == 2) {
          char marker[40];
          owner_name(child.name, marker);
          if (unlinkat(parent, marker, 0) == 0)
            ++removed;
          else if (errno != ENOENT) {
            ++errors;
            child.failed = true;
          }
        }
      } else {
        ++errors;
        child.failed = true;
      }
    }
    if (child.failed) frames[depth - 2].failed = true;
    child = Frame{};
    --depth;
  }

  void entry(const char *name) {
    const int parent = dirfd(frames[depth - 1].cursor);
    if (depth == 1) {
      if (boot == name) return;
      // Orphan markers are safe to remove after their entire boot tree is gone.
      if (std::strlen(name) == 32 + sizeof(kOwnerSuffix) - 1 &&
          std::strcmp(name + 32, kOwnerSuffix) == 0) {
        char old_boot[33];
        std::memcpy(old_boot, name, 32);
        old_boot[32] = 0;
        if (boot == old_boot || !boot_name(old_boot) ||
            !owned_boot(parent, old_boot))
          return;
        struct stat info;
        if (fstatat(parent, old_boot, &info, AT_SYMLINK_NOFOLLOW) != 0 &&
            errno == ENOENT) {
          if (unlinkat(parent, name, 0) == 0)
            ++removed;
          else if (errno != ENOENT)
            error();
        }
        return;
      }
      if (!boot_name(name) || !owned_boot(parent, name)) return;
    }
    struct stat info;
    if (fstatat(parent, name, &info, AT_SYMLINK_NOFOLLOW) != 0) {
      if (errno != ENOENT) error();
      return;
    }
    if (S_ISDIR(info.st_mode)) {
      if (depth == kDepth || std::strlen(name) >= kName) {
        error();
        return;
      }
      const int fd = open_directory(parent, name);
      if (fd < 0 || !push(fd, name)) error();
    } else {
      // unlinkat does not follow symlinks, including a substituted boot link.
      if (unlinkat(parent, name, 0) == 0)
        ++removed;
      else if (errno != ENOENT)
        error();
    }
  }
};
Scanner scanner;
}  // namespace

void preserve_trx_temp_gc_startup(const std::string &root) {
  scanner.close();
  scanner.root = normalized_root(root);
  scanner.boot = preserve_trx_transfer_receiver_boot_incarnation();
}

bool preserve_trx_temp_receiver_process_dir(const std::string &parent,
                                            std::string *directory) {
  if (directory == nullptr || parent.empty()) return false;
  const std::string boot = preserve_trx_transfer_receiver_boot_incarnation();
  if (!boot_name(boot.c_str())) return false;
  // This lock only serializes the few mkdir/marker operations. The reaper
  // never takes it and cannot delay a receiver on old-tree traversal.
  std::lock_guard<std::mutex> lock(directory_mutex);
  const std::string canonical = normalized_root(parent);
  if (canonical == cached_parent) {
    *directory = cached_directory;
    return true;
  }
  auto files = preserve_trx_acquire_file_resource_lease(canonical, 3, 0);
  if (!files.acquired()) return false;
  const int root = open_directory(AT_FDCWD, canonical.c_str());
  if (root < 0) return false;
  const auto close_root = create_scope_guard([&] { close(root); });
  if (!ensure_directory(root, kPrivate)) return false;
  const int private_fd = open_directory(root, kPrivate);
  if (private_fd < 0) return false;
  const auto close_private = create_scope_guard([&] { close(private_fd); });
  struct stat info;
  if (fstatat(private_fd, boot.c_str(), &info, AT_SYMLINK_NOFOLLOW) == 0) {
    if (!S_ISDIR(info.st_mode) || !owned_boot(private_fd, boot.c_str()))
      return false;
  } else if (errno != ENOENT || !create_owner(private_fd, boot.c_str()) ||
             mkdirat(private_fd, boot.c_str(), 0700) != 0) {
    return false;
  }
  *directory = canonical + "/" + kPrivate + "/" + boot;
  std::string next_parent(canonical), next_directory(*directory);
  cached_parent.swap(next_parent);
  cached_directory.swap(next_directory);
  return true;
}

bool preserve_trx_temp_gc_private_entry(const char *name) {
  return name != nullptr && std::strcmp(name, kPrivate) == 0;
}

void preserve_trx_temp_gc_step(size_t entry_budget) {
  try {
    if (entry_budget == 0 || (scanner.depth == 0 && !scanner.open())) return;
    while (entry_budget-- != 0 && scanner.depth != 0) {
      errno = 0;
      const dirent *entry = readdir(scanner.frames[scanner.depth - 1].cursor);
      if (entry == nullptr) {
        if (errno != 0) scanner.error();
        scanner.finish_directory();
        continue;
      }
      ++scanned;
      if (std::strcmp(entry->d_name, ".") != 0 &&
          std::strcmp(entry->d_name, "..") != 0)
        scanner.entry(entry->d_name);
    }
  } catch (const std::bad_alloc &) {
    ++errors;
    scanner.close();
  }
}

void preserve_trx_temp_gc_stop() { scanner.close(); }
ulonglong preserve_trx_temp_gc_scanned_status() { return scanned.load(); }
ulonglong preserve_trx_temp_gc_removed_status() { return removed.load(); }
ulonglong preserve_trx_temp_gc_errors_status() { return errors.load(); }
ulonglong preserve_trx_temp_gc_passes_status() { return passes.load(); }
