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

//! A failed `INSERT INTO` must leave no data files behind: they are never
//! committed, so nothing would ever read or delete them.

use std::error::Error;
use std::path::Path;
use std::sync::Arc;

use datafusion::execution::context::{SessionConfig, SessionContext};
use iceberg::TableIdent;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Transform, Type};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_datafusion_test_tools::{FixtureCatalog, FixtureTable};

/// Source rows with exactly the table's column types, in batches of 50.
const CREATE_SOURCE: &str = "CREATE TABLE src AS \
    SELECT CAST(value AS INT) AS id, value AS v, value % 3 AS p \
    FROM generate_series(1, 10000)";

/// Fails with a divide by zero at row 5000, after many batches have been
/// written, with several write partitions. The failure is in a filter, so the
/// partition projection is not merged into a projection below it (see
/// apache/datafusion-iceberg#92).
const FAILING_INSERT: &str = "INSERT INTO catalog.ns.t \
    SELECT * FROM src WHERE 10 / (v - 5000) > -100";

fn schema() -> Result<Schema, Box<dyn Error>> {
    Ok(Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
            NestedField::required(2, "v", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::required(3, "p", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()?)
}

async fn context(fixtures: &FixtureCatalog) -> SessionContext {
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(4)
            .with_batch_size(50),
    );
    ctx.register_catalog(
        "catalog",
        Arc::new(
            IcebergCatalogProvider::try_new(fixtures.catalog())
                .await
                .unwrap(),
        ),
    );
    ctx
}

/// Counts the Parquet files under the table's data directory.
fn data_files(table: &FixtureTable) -> usize {
    let location = table.table().metadata().location();
    let location = location
        .strip_prefix("file://")
        .or_else(|| location.strip_prefix("file:"))
        .unwrap_or(location);
    fn count(dir: &Path) -> usize {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return 0;
        };
        entries
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    count(&path)
                } else {
                    usize::from(path.extension().is_some_and(|e| e == "parquet"))
                }
            })
            .sum()
    }
    count(&Path::new(location).join("data"))
}

async fn row_count(ctx: &SessionContext) -> Result<i64, Box<dyn Error>> {
    let batches = ctx
        .sql("SELECT count(*) FROM catalog.ns.t")
        .await?
        .collect()
        .await?;
    let count = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .unwrap()
        .value(0);
    Ok(count)
}

async fn assert_failed_insert_leaves_no_files(
    table: FixtureTable,
    fixtures: FixtureCatalog,
) -> Result<(), Box<dyn Error>> {
    let ctx = context(&fixtures).await;
    ctx.sql(CREATE_SOURCE).await?.collect().await?;

    let err = ctx.sql(FAILING_INSERT).await?.collect().await.unwrap_err();
    assert!(err.to_string().contains("Divide by zero"), "{err}");

    // The deletes of cancelled partitions run in background tasks.
    for _ in 0..10_000 {
        if data_files(&table) == 0 {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert_eq!(data_files(&table), 0);
    assert_eq!(row_count(&ctx).await?, 0);

    // A later INSERT still works.
    ctx.sql("INSERT INTO catalog.ns.t SELECT * FROM src WHERE id = 1")
        .await?
        .collect()
        .await?;
    assert_eq!(row_count(&ctx).await?, 1);
    assert!(data_files(&table) > 0);
    Ok(())
}

#[tokio::test]
async fn failed_insert_leaves_no_data_files() -> Result<(), Box<dyn Error>> {
    let fixtures = FixtureCatalog::try_new().await?;
    let table = fixtures
        .table(TableIdent::from_strs(["ns", "t"])?, schema()?)
        // Small files, so files are completed before the failure.
        .property("write.target-file-size-bytes", "1024")
        .create()
        .await?;
    assert_failed_insert_leaves_no_files(table, fixtures).await
}

#[tokio::test]
async fn failed_partitioned_insert_leaves_no_data_files() -> Result<(), Box<dyn Error>> {
    let fixtures = FixtureCatalog::try_new().await?;
    let table = fixtures
        .table(TableIdent::from_strs(["ns", "t"])?, schema()?)
        .partition_by("p", Transform::Identity)
        .property("write.target-file-size-bytes", "1024")
        .create()
        .await?;
    assert_failed_insert_leaves_no_files(table, fixtures).await
}
