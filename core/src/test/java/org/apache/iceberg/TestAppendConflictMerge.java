/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg;

import static org.apache.iceberg.expressions.Expressions.bucket;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestAppendConflictMerge extends TestBase {

  @Parameters(name = "formatVersion = {0}, appendMode = {1}")
  protected static List<Object[]> parameters() {
    return TestHelpers.ALL_VERSIONS.stream()
        .flatMap(
            version ->
                Stream.of(
                    new Object[] {version, AppendMode.MERGE},
                    new Object[] {version, AppendMode.FAST}))
        .collect(Collectors.toList());
  }

  @Parameter(index = 1)
  private AppendMode appendMode;

  private enum AppendMode {
    MERGE,
    FAST
  }

  private AppendFiles newAppend(Table tbl) {
    return appendMode == AppendMode.FAST ? tbl.newFastAppend() : tbl.newAppend();
  }

  private void enableConflictMerge() {
    table
        .updateProperties()
        .set(TableProperties.COMMIT_CONFLICT_MERGE_ENABLED, "true")
        // production catalogs assign random snapshot ids; the in-memory test ops otherwise hand out
        // sequential ids that can collide across separate table handles during a retry
        .set("random-snapshot-ids", "true")
        .commit();
  }

  private List<CharSequence> scannedFilePaths() {
    Table refreshed = load();
    return Lists.newArrayList(
        Iterables.transform(refreshed.newScan().planFiles(), task -> task.file().location()));
  }

  @TestTemplate
  public void testRetriedAppendSkipsFilesAlreadyCommitted() {
    enableConflictMerge();

    table.newAppend().appendFile(FILE_A).commit();

    // a separate writer handle models another task whose view of the table is stale
    TestTables.TestTable concurrentWriter = load();
    AppendFiles retryAppend = newAppend(concurrentWriter);
    retryAppend.appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(scannedFilePaths()).containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
    Snapshot committed = load().currentSnapshot();
    assertThat(committed.summary())
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1")
        .containsEntry(SnapshotSummary.ADDED_RECORDS_PROP, "1");
  }

  @TestTemplate
  public void testRetriedAppendWithAllFilesCommittedAddsNoFiles() {
    enableConflictMerge();

    table.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();
    long parentId = table.currentSnapshot().snapshotId();

    TestTables.TestTable concurrentWriter = load();
    AppendFiles retryAppend = newAppend(concurrentWriter);
    retryAppend.appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(scannedFilePaths()).containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location());
    Snapshot committed = load().currentSnapshot();
    assertThat(committed.parentId()).isEqualTo(parentId);
    assertThat(committed.summary()).doesNotContainKey(SnapshotSummary.ADDED_FILES_PROP);
  }

  @TestTemplate
  public void testConflictMergeDisabledKeepsDuplicateFiles() {
    table.newAppend().appendFile(FILE_A).commit();

    TestTables.TestTable concurrentWriter = load();
    AppendFiles retryAppend = newAppend(concurrentWriter);
    retryAppend.appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(scannedFilePaths())
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_A.location(), FILE_B.location());
  }

  @TestTemplate
  public void testConflictMergeWithEvolvedPartitionSpec() {
    enableConflictMerge();

    table.newAppend().appendFile(FILE_A).commit();
    table.updateSpec().addField(bucket("id", 8)).commit();
    PartitionSpec newSpec = table.spec();

    DataFile newSpecFile =
        DataFiles.builder(newSpec)
            .withPath("/path/to/data-new-spec.parquet")
            .withPartitionPath("data_bucket=0/id_bucket_8=0")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();

    // the stale writer still uses the old spec for FILE_A and also adds a new-spec file
    TestTables.TestTable concurrentWriter = load();
    AppendFiles retryAppend = newAppend(concurrentWriter);
    retryAppend.appendFile(FILE_A).appendFile(newSpecFile).commit();

    assertThat(scannedFilePaths())
        .containsExactlyInAnyOrder(FILE_A.location(), newSpecFile.location());
    assertThat(load().currentSnapshot().summary())
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1");
  }

  @TestTemplate
  public void testConflictMergeAcrossMultipleConcurrentCommits() {
    enableConflictMerge();

    table.newAppend().appendFile(FILE_A).commit();
    TestTables.TestTable staleWriter = load();

    // advance the table again while the stale writer holds its old metadata
    table.newAppend().appendFile(FILE_B).commit();

    AppendFiles retryAppend = newAppend(staleWriter);
    retryAppend.appendFile(FILE_A).appendFile(FILE_B).appendFile(FILE_C).commit();

    assertThat(scannedFilePaths())
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_B.location(), FILE_C.location());
    assertThat(load().currentSnapshot().summary())
        .containsEntry(SnapshotSummary.ADDED_FILES_PROP, "1");
  }

  @TestTemplate
  public void testConflictMergeCanBeDisabledAgain() {
    enableConflictMerge();
    table.newAppend().appendFile(FILE_A).commit();

    table.updateProperties().set(TableProperties.COMMIT_CONFLICT_MERGE_ENABLED, "false").commit();

    TestTables.TestTable concurrentWriter = load();
    AppendFiles retryAppend = newAppend(concurrentWriter);
    retryAppend.appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(scannedFilePaths())
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_A.location(), FILE_B.location());
  }

  @TestTemplate
  public void testAppendManifestIsNotDeduplicated() throws IOException {
    enableConflictMerge();

    table.newAppend().appendFile(FILE_A).commit();

    // appending a pre-built manifest bypasses the per-file dedup path, so its files are retained
    ManifestFile manifest = writeManifest(FILE_A, FILE_B);
    TestTables.TestTable concurrentWriter = load();
    newAppend(concurrentWriter).appendManifest(manifest).commit();

    assertThat(scannedFilePaths())
        .containsExactlyInAnyOrder(FILE_A.location(), FILE_A.location(), FILE_B.location());
  }
}
