/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;

/**
 * The tables a {@link TriggerReconciler#reconcile} call touched, grouped by what changed.
 *
 * @param created tables that had no capture triggers installed before this reconcile
 * @param altered tables whose capture triggers were installed but did not match their columns
 * @param dropped tables no longer monitored whose orphaned capture triggers were removed
 */
public record ReconcileResult(List<String> created, List<String> altered, List<String> dropped) {
}
