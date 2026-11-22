// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Process-level resource limits and a periodic journal of the resources really used.
//!
//! A process that is killed by the kernel or frozen by resource exhaustion leaves no trace of the
//! reason in its own log. The journal writes, from a plain OS thread that does not depend on any
//! async runtime, what the process was using shortly before: resident memory, open file
//! descriptors and the limits they are measured against.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Interval between two journal lines. `0` disables the journal.
const INTERVAL_ENV: &str = "RW_RESOURCE_JOURNAL_INTERVAL_SECS";
const DEFAULT_INTERVAL_SECS: u64 = 60;

/// Set by [`disable_resource_journal`].
static DISABLED: AtomicBool = AtomicBool::new(false);

/// Usage ratio above which a journal line is written as a warning.
const WARN_RATIO: f64 = 0.8;

/// A delay of the journal thread beyond its interval that is reported as a stall of the process.
const STALL_THRESHOLD: Duration = Duration::from_secs(5);

/// Raises the soft limit of open file descriptors to the hard limit and logs both.
///
/// Container runtimes often start processes with a low soft limit (1024) and a much higher hard
/// limit. Spilling batch operators, object store connections and client connections all draw from
/// this limit, and exhausting it makes the server refuse new connections.
pub fn raise_nofile_limit() {
    let before = rlimit::Resource::NOFILE.get();
    match rlimit::increase_nofile_limit(u64::MAX) {
        Ok(soft) => match before {
            Ok((old_soft, hard)) if old_soft != soft => tracing::info!(
                old_soft,
                soft,
                hard,
                "raised the soft limit of open file descriptors"
            ),
            Ok((_, hard)) => tracing::debug!(soft, hard, "limit of open file descriptors"),
            Err(_) => tracing::debug!(soft, "limit of open file descriptors"),
        },
        Err(e) => tracing::warn!(
            error = %e,
            ?before,
            "failed to raise the soft limit of open file descriptors"
        ),
    }
}

/// The soft limit of open file descriptors of the process, if it can be read and is finite.
pub fn nofile_soft_limit() -> Option<u64> {
    match rlimit::Resource::NOFILE.get_soft() {
        Ok(rlimit::INFINITY) | Err(_) => None,
        Ok(soft) => Some(soft),
    }
}

/// The number of file descriptors currently open in the process. Linux only.
pub fn open_fd_count() -> Option<u64> {
    let dir = std::fs::read_dir("/proc/self/fd").ok()?;
    // The directory handle used for the listing is itself one of the entries.
    Some((dir.count() as u64).saturating_sub(1))
}

/// Reads a `kB` field such as `VmRSS` or `MemAvailable` from a `/proc` status-like file.
fn read_kb_field(path: &str, field: &str) -> Option<u64> {
    let content = std::fs::read_to_string(path).ok()?;
    parse_kb_field(&content, field)
}

fn parse_kb_field(content: &str, field: &str) -> Option<u64> {
    content.lines().find_map(|line| {
        let rest = line.strip_prefix(field)?.strip_prefix(':')?;
        let kb: u64 = rest.trim().strip_suffix("kB")?.trim().parse().ok()?;
        Some(kb * 1024)
    })
}

/// Reads a cgroup v2 value of the cgroup of the process, `None` when absent or `max`.
fn read_cgroup_v2(file: &str) -> Option<u64> {
    std::fs::read_to_string(format!("/sys/fs/cgroup/{file}"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Snapshot {
    /// Resident memory of the process.
    rss_bytes: Option<u64>,
    /// Memory charged to the cgroup of the process, page cache included.
    cgroup_memory_bytes: Option<u64>,
    /// Memory limit of the cgroup, if any.
    cgroup_memory_limit_bytes: Option<u64>,
    /// Memory of the machine and what the kernel considers still available.
    system_memory_bytes: Option<u64>,
    system_memory_available_bytes: Option<u64>,
    open_fds: Option<u64>,
    nofile_soft_limit: Option<u64>,
}

impl Snapshot {
    fn take() -> Self {
        Self {
            rss_bytes: read_kb_field("/proc/self/status", "VmRSS"),
            cgroup_memory_bytes: read_cgroup_v2("memory.current"),
            cgroup_memory_limit_bytes: read_cgroup_v2("memory.max"),
            system_memory_bytes: read_kb_field("/proc/meminfo", "MemTotal"),
            system_memory_available_bytes: read_kb_field("/proc/meminfo", "MemAvailable"),
            open_fds: open_fd_count(),
            nofile_soft_limit: nofile_soft_limit(),
        }
    }

    /// The highest usage ratio among the resources that have a limit.
    fn max_ratio(&self) -> f64 {
        let ratio = |used: Option<u64>, limit: Option<u64>| match (used, limit) {
            (Some(used), Some(limit)) if limit > 0 => used as f64 / limit as f64,
            _ => 0.0,
        };
        let system_used = match (self.system_memory_bytes, self.system_memory_available_bytes) {
            (Some(total), Some(available)) => Some(total.saturating_sub(available)),
            _ => None,
        };
        ratio(self.open_fds, self.nofile_soft_limit)
            .max(ratio(
                self.cgroup_memory_bytes,
                self.cgroup_memory_limit_bytes,
            ))
            .max(ratio(system_used, self.system_memory_bytes))
    }

    fn log(&self) {
        let mb = |bytes: Option<u64>| bytes.map(|b| b >> 20);
        let ratio = self.max_ratio();
        macro_rules! emit {
            ($macro:ident) => {
                tracing::$macro!(
                    rss_mb = ?mb(self.rss_bytes),
                    cgroup_memory_mb = ?mb(self.cgroup_memory_bytes),
                    cgroup_memory_limit_mb = ?mb(self.cgroup_memory_limit_bytes),
                    system_memory_mb = ?mb(self.system_memory_bytes),
                    system_memory_available_mb = ?mb(self.system_memory_available_bytes),
                    open_fds = ?self.open_fds,
                    nofile_soft_limit = ?self.nofile_soft_limit,
                    max_usage_ratio = format_args!("{ratio:.2}"),
                    "resource journal"
                )
            };
        }
        if ratio >= WARN_RATIO {
            emit!(warn);
        } else {
            emit!(info);
        }
    }
}

/// Keeps [`spawn_resource_journal`] from starting the journal. For short-lived command line tools,
/// whose output must not be mixed with journal lines.
pub fn disable_resource_journal() {
    DISABLED.store(true, Ordering::Relaxed);
}

/// Starts the thread that periodically writes the resources used by the process to the log.
///
/// The interval is read from `RW_RESOURCE_JOURNAL_INTERVAL_SECS` (default 60, `0` to disable).
/// While a resource is above 80% of its limit, a line is written every 5 seconds instead.
pub fn spawn_resource_journal() {
    if DISABLED.load(Ordering::Relaxed) {
        return;
    }
    let interval_secs = match std::env::var(INTERVAL_ENV) {
        Ok(v) => match v.parse::<u64>() {
            Ok(secs) => secs,
            Err(_) => {
                tracing::warn!("invalid {INTERVAL_ENV} `{v}`, using {DEFAULT_INTERVAL_SECS}");
                DEFAULT_INTERVAL_SECS
            }
        },
        Err(_) => DEFAULT_INTERVAL_SECS,
    };
    if interval_secs == 0 {
        return;
    }
    let interval = Duration::from_secs(interval_secs);
    let fast_interval = interval.min(STALL_THRESHOLD);

    let spawned = std::thread::Builder::new()
        .name("rw-resource-journal".to_owned())
        .spawn(move || {
            loop {
                let snapshot = Snapshot::take();
                snapshot.log();
                let under_pressure = snapshot.max_ratio() >= WARN_RATIO;

                let sleep = if under_pressure {
                    fast_interval
                } else {
                    interval
                };
                let start = Instant::now();
                std::thread::sleep(sleep);
                let late = start.elapsed().saturating_sub(sleep);
                if late >= STALL_THRESHOLD {
                    tracing::warn!(
                        late_ms = late.as_millis() as u64,
                        "resource journal thread woke up late: the process or the host was stalled"
                    );
                }
            }
        });
    if let Err(e) = spawned {
        tracing::warn!(error = %e, "failed to start the resource journal thread");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_kb_field() {
        let status = "Name:\trisingwave\nVmRSS:\t  123456 kB\nVmSwap:\t       0 kB\n";
        assert_eq!(parse_kb_field(status, "VmRSS"), Some(123456 * 1024));
        assert_eq!(parse_kb_field(status, "VmSwap"), Some(0));
        assert_eq!(parse_kb_field(status, "VmHWM"), None);
        // A field name that is only a prefix of another one must not match.
        assert_eq!(parse_kb_field(status, "Vm"), None);
    }

    #[test]
    fn test_max_ratio() {
        let snapshot = Snapshot {
            open_fds: Some(900),
            nofile_soft_limit: Some(1000),
            cgroup_memory_bytes: Some(10),
            cgroup_memory_limit_bytes: Some(100),
            system_memory_bytes: Some(1000),
            system_memory_available_bytes: Some(500),
            rss_bytes: None,
        };
        assert!((snapshot.max_ratio() - 0.9).abs() < 1e-9);
        // Resources without a limit do not count.
        assert_eq!(Snapshot::default().max_ratio(), 0.0);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn test_open_fd_count() {
        // Other tests open and close files concurrently, so only check that the count is sane.
        let _file = std::fs::File::open("/proc/self/status").unwrap();
        let count = open_fd_count().unwrap();
        assert!(count >= 4, "stdin, stdout, stderr and one file: {count}");
    }
}
