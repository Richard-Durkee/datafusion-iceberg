// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Deletes the data files of an INSERT that failed before it was committed.
//!
//! An Iceberg write makes its data files visible only in the final commit, so
//! a failed INSERT never changes the table. But the data files it already
//! wrote stay in the table location, where nothing references them. These
//! helpers delete them, so a failed INSERT does not leave orphan files behind.
//!
//! Files are never deleted once a commit has been attempted: a commit that
//! returns an error may still have succeeded.

use std::sync::{Arc, Mutex};

use iceberg::io::FileIO;
use iceberg::spec::PartitionKey;
use iceberg::writer::file_writer::location_generator::LocationGenerator;

/// Deletes `paths`, ignoring files that do not exist.
///
/// This is best effort: a file that cannot be deleted is left behind, as it
/// would have been without this cleanup.
pub(crate) async fn delete_data_files(file_io: &FileIO, paths: Vec<String>) {
    for path in paths {
        let _ = file_io.delete(&path).await;
    }
}

/// A [`LocationGenerator`] that records every location it hands out, so that
/// the files of a failed write can be deleted. A location is recorded before
/// its file is created.
#[derive(Clone)]
pub(crate) struct TrackingLocationGenerator<L> {
    inner: L,
    created: Arc<Mutex<Vec<String>>>,
}

impl<L: LocationGenerator> TrackingLocationGenerator<L> {
    pub(crate) fn new(inner: L) -> Self {
        Self {
            inner,
            created: Arc::default(),
        }
    }

    /// Returns a guard that deletes every recorded file unless it is disarmed.
    pub(crate) fn cleanup_guard(&self, file_io: FileIO) -> CleanupGuard {
        CleanupGuard {
            file_io,
            created: Some(Arc::clone(&self.created)),
        }
    }
}

impl<L: LocationGenerator> LocationGenerator for TrackingLocationGenerator<L> {
    fn generate_location(
        &self,
        partition_key: Option<&PartitionKey>,
        file_name: &str,
    ) -> String {
        let location = self.inner.generate_location(partition_key, file_name);
        self.created.lock().unwrap().push(location.clone());
        location
    }
}

/// Deletes the files recorded by a [`TrackingLocationGenerator`].
///
/// Call [`CleanupGuard::delete`] when the write fails, or
/// [`CleanupGuard::disarm`] once the files have been handed on to the commit.
/// If the guard is dropped while still armed, for example because the query
/// was cancelled, the files are deleted in a background task.
pub(crate) struct CleanupGuard {
    file_io: FileIO,
    created: Option<Arc<Mutex<Vec<String>>>>,
}

impl CleanupGuard {
    /// Keeps the files: they are now the commit's responsibility.
    pub(crate) fn disarm(mut self) {
        self.created = None;
    }

    /// Deletes every file written so far.
    pub(crate) async fn delete(mut self) {
        if let Some(created) = self.created.take() {
            let paths = std::mem::take(&mut *created.lock().unwrap());
            delete_data_files(&self.file_io, paths).await;
        }
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let Some(created) = self.created.take() else {
            return;
        };
        let paths = std::mem::take(&mut *created.lock().unwrap());
        if paths.is_empty() {
            return;
        }
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let file_io = self.file_io.clone();
            handle.spawn(async move { delete_data_files(&file_io, paths).await });
        }
    }
}
