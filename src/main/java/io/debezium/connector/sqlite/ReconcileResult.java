/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

import java.util.List;

/** The tables a {@link TriggerReconciler#reconcile} call created, altered, or dropped. */
public record ReconcileResult(List<String> created, List<String> altered, List<String> dropped) {
}
