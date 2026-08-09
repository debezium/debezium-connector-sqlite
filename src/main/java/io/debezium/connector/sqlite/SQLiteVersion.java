/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.sqlite;

/**
 * Compares dot-separated SQLite version strings.
 */
final class SQLiteVersion {

    private SQLiteVersion() {
    }

    /**
     * Returns whether an {@code X.Y.Z} version is at or above a minimum. A null or unparseable
     * version is treated as below the minimum so a version guard fails closed.
     *
     * @param version the version string to test, as reported by {@code sqlite_version()}
     * @param minimum the inclusive lower bound, as an {@code X.Y.Z} string
     * @return true if {@code version} is at least {@code minimum}, false otherwise
     */
    static boolean isAtLeast(String version, String minimum) {
        if (version == null) {
            return false;
        }
        int[] versionParts = parseParts(version);
        int[] minimumParts = parseParts(minimum);
        for (int i = 0; i < minimumParts.length; i++) {
            int part = i < versionParts.length ? versionParts[i] : 0;
            if (part != minimumParts[i]) {
                return part > minimumParts[i];
            }
        }
        return true;
    }

    /** Splits an {@code X.Y.Z} version string into the numeric parts of its dot-separated tokens. */
    private static int[] parseParts(String version) {
        String[] tokens = version.trim().split("\\.");
        int[] parts = new int[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            parts[i] = parseLeadingInt(tokens[i]);
        }
        return parts;
    }

    /**
     * Parses the leading run of digits in a version token, returning -1 if the token does not start
     * with a digit. Comparing -1 against any minimum part fails the version guard.
     */
    private static int parseLeadingInt(String token) {
        int end = 0;
        while (end < token.length() && Character.isDigit(token.charAt(end))) {
            end++;
        }
        return end == 0 ? -1 : Integer.parseInt(token.substring(0, end));
    }
}
