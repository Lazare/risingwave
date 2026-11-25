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

//! A bounded pool of Postgres connections for the Postgres sink.
//!
//! The sinks of a group (see [`crate::sink::sink_group`]) running in the same process share one
//! pool, so that the number of connections to Postgres is bounded by the pool size instead of
//! growing with the number of sink actors. A connection is taken for the duration of a
//! transaction, and a broken connection is replaced the next time one is acquired.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, LazyLock, Weak};

use anyhow::Context;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_postgres::{Client, Statement};

use crate::connector_common::{PgConnectionConfig, TcpKeepaliveConfig, create_pg_client};

pub const DEFAULT_POOL_MAX_SIZE: usize = 4;

/// Identifies the pool shared by the sinks of a group: the group and the connection settings.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct PoolKey {
    group: String,
    host: String,
    port: u16,
    user: String,
    password: String,
    database: String,
    ssl_mode: String,
    ssl_root_cert: Option<String>,
}

impl PoolKey {
    fn new(group: &str, config: &PgConnectionConfig) -> Self {
        Self {
            group: group.to_owned(),
            host: config.host.clone(),
            port: config.port,
            user: config.user.clone(),
            password: config.password.clone(),
            database: config.database.clone(),
            ssl_mode: config.ssl_mode.to_string(),
            ssl_root_cert: config.ssl_root_cert.clone(),
        }
    }
}

static SHARED_POOLS: LazyLock<Mutex<HashMap<PoolKey, Weak<PgPool>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// A connection of the pool with the statements prepared on it.
pub struct PgPooledClient {
    client: Client,
    statements: HashMap<String, Statement>,
}

impl PgPooledClient {
    /// Prepares a statement once per connection.
    pub async fn prepare_cached(&mut self, sql: &str) -> anyhow::Result<Statement> {
        if let Some(statement) = self.statements.get(sql) {
            return Ok(statement.clone());
        }
        let statement = self
            .client
            .prepare(sql)
            .await
            .with_context(|| format!("failed to prepare statement: {}", sql))?;
        self.statements.insert(sql.to_owned(), statement.clone());
        Ok(statement)
    }
}

impl Deref for PgPooledClient {
    type Target = Client;

    fn deref(&self) -> &Client {
        &self.client
    }
}

impl DerefMut for PgPooledClient {
    fn deref_mut(&mut self) -> &mut Client {
        &mut self.client
    }
}

pub struct PgPool {
    config: PgConnectionConfig,
    tcp_keepalive: Option<TcpKeepaliveConfig>,
    max_size: usize,
    permits: Arc<Semaphore>,
    idle: Mutex<Vec<PgPooledClient>>,
}

impl PgPool {
    /// A pool that is not shared with other sinks.
    pub fn new(
        config: PgConnectionConfig,
        tcp_keepalive: Option<TcpKeepaliveConfig>,
        max_size: usize,
    ) -> Arc<Self> {
        let max_size = max_size.max(1);
        Arc::new(Self {
            config,
            tcp_keepalive,
            max_size,
            permits: Arc::new(Semaphore::new(max_size)),
            idle: Mutex::new(Vec::new()),
        })
    }

    /// The pool shared by the sinks of `group` with the same connection settings in this process.
    /// It lives as long as one sink holds it. `max_size` only applies when the pool is created.
    pub fn shared(
        group: &str,
        config: PgConnectionConfig,
        tcp_keepalive: Option<TcpKeepaliveConfig>,
        max_size: usize,
    ) -> Arc<Self> {
        let key = PoolKey::new(group, &config);
        let mut pools = SHARED_POOLS.lock();
        pools.retain(|_, pool| pool.strong_count() > 0);
        if let Some(pool) = pools.get(&key).and_then(Weak::upgrade) {
            if pool.max_size != max_size.max(1) {
                tracing::warn!(
                    group,
                    current = pool.max_size,
                    requested = max_size,
                    "postgres sink pool already exists with another size, keeping the current one"
                );
            }
            return pool;
        }
        let pool = Self::new(config, tcp_keepalive, max_size);
        pools.insert(key, Arc::downgrade(&pool));
        pool
    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    /// Takes a connection, waiting for one to be released when the pool is exhausted. A broken
    /// idle connection is dropped and a new one is opened instead.
    pub async fn acquire(self: &Arc<Self>) -> anyhow::Result<PgPoolGuard> {
        let permit = self
            .permits
            .clone()
            .acquire_owned()
            .await
            .expect("semaphore is never closed");
        let client = loop {
            let idle = self.idle.lock().pop();
            match idle {
                Some(client) if client.is_closed() => {
                    tracing::warn!("dropping a closed postgres sink connection");
                }
                Some(client) => break client,
                None => {
                    let client = create_pg_client(&self.config, self.tcp_keepalive.clone()).await?;
                    break PgPooledClient {
                        client,
                        statements: HashMap::new(),
                    };
                }
            }
        };
        Ok(PgPoolGuard {
            client: Some(client),
            pool: self.clone(),
            _permit: permit,
        })
    }
}

/// A connection taken from the pool, returned to it when dropped unless it is broken.
pub struct PgPoolGuard {
    client: Option<PgPooledClient>,
    pool: Arc<PgPool>,
    _permit: OwnedSemaphorePermit,
}

impl PgPoolGuard {
    /// Closes the connection instead of returning it to the pool, when its state is unknown
    /// (after an error).
    pub fn discard(mut self) {
        self.client = None;
    }
}

impl Deref for PgPoolGuard {
    type Target = PgPooledClient;

    fn deref(&self) -> &PgPooledClient {
        self.client.as_ref().expect("client is present until drop")
    }
}

impl DerefMut for PgPoolGuard {
    fn deref_mut(&mut self) -> &mut PgPooledClient {
        self.client.as_mut().expect("client is present until drop")
    }
}

impl Drop for PgPoolGuard {
    fn drop(&mut self) {
        if let Some(client) = self.client.take()
            && !client.is_closed()
        {
            self.pool.idle.lock().push(client);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connector_common::SslMode;

    fn config(database: &str) -> PgConnectionConfig {
        PgConnectionConfig {
            host: "localhost".to_owned(),
            port: 5432,
            user: "postgres".to_owned(),
            password: String::new(),
            database: database.to_owned(),
            ssl_mode: SslMode::Disabled,
            ssl_root_cert: None,
        }
    }

    #[test]
    fn test_shared_pool_is_keyed_by_group_and_settings() {
        let a1 = PgPool::shared("test-pool-a", config("db"), None, 2);
        let a2 = PgPool::shared("test-pool-a", config("db"), None, 2);
        assert!(Arc::ptr_eq(&a1, &a2));
        assert_eq!(a1.max_size(), 2);

        let other_group = PgPool::shared("test-pool-b", config("db"), None, 2);
        assert!(!Arc::ptr_eq(&a1, &other_group));
        let other_settings = PgPool::shared("test-pool-a", config("db2"), None, 2);
        assert!(!Arc::ptr_eq(&a1, &other_settings));

        // The pool is dropped with its last user, and created again on demand.
        let weak = Arc::downgrade(&a1);
        drop(a1);
        drop(a2);
        assert!(weak.upgrade().is_none());
        let a3 = PgPool::shared("test-pool-a", config("db"), None, 3);
        assert_eq!(a3.max_size(), 3);
    }
}
