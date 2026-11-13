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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.DataFileSet;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Merges pending appends with concurrently committed snapshots by dropping pending data files that
 * are already present in the table.
 *
 * <p>Files are identified by their fully qualified path, so a retried write is merged instead of
 * being appended twice. Files are matched per partition spec, which keeps appends written against
 * older partition specs working while the table spec evolves.
 */
class AppendConflictResolver {
  private static final Logger LOG = LoggerFactory.getLogger(AppendConflictResolver.class);

  private AppendConflictResolver() {}

  /**
   * Returns the pending data files grouped by partition spec after dropping files already present
   * in the given snapshot.
   *
   * @param io a {@link FileIO} used to read the snapshot manifests
   * @param base table metadata to validate against
   * @param parent current head snapshot of the target branch, or null for an empty table
   * @param pendingFilesBySpec data files the append is about to commit, grouped by spec id
   * @return the files still missing from the table, grouped by spec id; null when nothing changed
   */
  static Map<Integer, DataFileSet> mergeCommittedFiles(
      FileIO io,
      TableMetadata base,
      Snapshot parent,
      Map<Integer, DataFileSet> pendingFilesBySpec) {
    Map<Integer, DataFileSet> mergedBySpec = Maps.newLinkedHashMap();
    boolean changed = false;
    for (Map.Entry<Integer, DataFileSet> entry : pendingFilesBySpec.entrySet()) {
      List<DataFile> newFiles =
          filterCommittedFiles(io, base.specsById(), base, parent, entry.getValue());
      if (newFiles.size() != entry.getValue().size()) {
        changed = true;
      }

      if (!newFiles.isEmpty()) {
        DataFileSet dataFiles = DataFileSet.create();
        dataFiles.addAll(newFiles);
        mergedBySpec.put(entry.getKey(), dataFiles);
      }
    }

    return changed ? mergedBySpec : null;
  }

  /**
   * Returns the data files that are not already present as live files in the given snapshot.
   *
   * @param io a {@link FileIO} used to read the snapshot manifests
   * @param specsById all partition specs of the table metadata the append is validated against
   * @param parent the current head snapshot of the target branch, or null for an empty table
   * @param pendingFiles data files the append is about to commit
   * @return the pending files whose paths do not match any live table file
   */
  private static List<DataFile> filterCommittedFiles(
      FileIO io,
      Map<Integer, PartitionSpec> specsById,
      TableMetadata base,
      Snapshot parent,
      Iterable<DataFile> pendingFiles) {
    CharSequenceSet committedPaths = liveDataFilePaths(io, specsById, base, parent);
    if (committedPaths.isEmpty()) {
      return ImmutableList.copyOf(pendingFiles);
    }

    List<DataFile> newFiles = Lists.newArrayList();
    int duplicateCount = 0;
    for (DataFile pendingFile : pendingFiles) {
      if (isCommitted(committedPaths, pendingFile.location())) {
        duplicateCount += 1;
      } else {
        newFiles.add(pendingFile);
      }
    }

    if (duplicateCount > 0) {
      LOG.info(
          "Skipping {} data file(s) already committed by a concurrent operation", duplicateCount);
    }

    return newFiles;
  }

  @SuppressWarnings("CollectionUndefinedEquality")
  private static boolean isCommitted(CharSequenceSet committedPaths, CharSequence path) {
    return committedPaths.contains(path);
  }

  private static CharSequenceSet liveDataFilePaths(
      FileIO io, Map<Integer, PartitionSpec> specsById, TableMetadata base, Snapshot parent) {
    if (parent == null) {
      return CharSequenceSet.empty();
    }

    Set<Long> committedSnapshotIds = Sets.newHashSet();
    List<ManifestFile> dataManifests = Lists.newArrayList();
    for (Snapshot current : SnapshotUtil.ancestorsOf(parent.snapshotId(), base::snapshot)) {
      long currentId = current.snapshotId();
      committedSnapshotIds.add(currentId);
      for (ManifestFile manifest : current.dataManifests(io)) {
        // only manifests produced by committed snapshots are authoritative; this excludes
        // manifests left behind by the current writer's failed commit attempts
        if (manifest.snapshotId() != null && committedSnapshotIds.contains(manifest.snapshotId())) {
          dataManifests.add(manifest);
        }
      }
    }

    CharSequenceSet paths = CharSequenceSet.empty();
    try (CloseableIterable<ManifestEntry<DataFile>> entries =
        new ManifestGroup(io, dataManifests)
            .specsById(specsById)
            .ignoreDeleted()
            .filterManifestEntries(entry -> committedSnapshotIds.contains(entry.snapshotId()))
            .entries()) {
      for (ManifestEntry<DataFile> entry : entries) {
        // manifest readers reuse entry and file objects, so copy the path before retaining it
        paths.add(entry.file().location().toString());
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read committed data files", e);
    }

    return paths;
  }
}
