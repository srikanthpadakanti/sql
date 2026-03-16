/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_LOCATIONS_TYPE_CONFLICT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.ppl.PPLIntegTestCase;

public class CalciteUnionCommandIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    loadIndex(Index.ACCOUNT);
    loadIndex(Index.BANK);
    loadIndex(Index.TIME_TEST_DATA);
    loadIndex(Index.TIME_TEST_DATA2);
    loadIndex(Index.LOCATIONS_TYPE_CONFLICT);
  }

  // ========================================================================
  // Basic Union Functionality
  // ========================================================================

  @Test
  public void testBasicUnionTwoSubsearches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| union "
                    + "[search source=%s | where age < 30 | eval age_group = \\\"young\\\"] "
                    + "[search source=%s | where age >= 30 | eval age_group = \\\"adult\\\"] "
                    + "| stats count by age_group | sort age_group",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("age_group", null, "string"));
    verifyDataRows(result, rows(549L, "adult"), rows(451L, "young"));
  }

  @Test
  public void testUnionThreeSubsearches() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| union [search source=%s | where state = \\\"IL\\\" | eval region"
                    + " = \\\"Illinois\\\"] [search source=%s | where state = \\\"TN\\\" | eval"
                    + " region = \\\"Tennessee\\\"] [search source=%s | where state = \\\"CA\\\" |"
                    + " eval region = \\\"California\\\"] | stats count by region | sort region",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"), schema("region", null, "string"));
    verifyDataRows(result, rows(17L, "California"), rows(22L, "Illinois"), rows(25L, "Tennessee"));
  }

  @Test
  public void testUnionDirectTableNames() throws IOException {
    // Test union using direct table names (non-subsearch format)
    // Union returns ALL rows from both tables (UNION ALL semantics)
    JSONObject result =
        executeQuery(
            String.format(
                "| union %s, %s | where account_number = 1 | fields firstname, city",
                TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));

    verifySchema(result, schema("firstname", null, "string"), schema("city", null, "string"));

    // Should return 2 rows: one from ACCOUNT and one from BANK (UNION ALL preserves both)
    verifyDataRows(result, rows("Amber", "Brogan"), rows("Amber JOHnny", "Brogan"));
  }

  @Test
  public void testUnionMixedDirectTableAndSubsearch() throws IOException {
    // Can mix direct table names and subsearches
    JSONObject result =
        executeQuery(
            String.format(
                "| union %s, [search source=%s | where age > 30] | stats count() as total",
                TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));

    verifySchema(result, schema("total", null, "bigint"));
    // ACCOUNT: all rows + BANK filtered (age > 30)
    verifyDataRows(result, rows(1006L));
  }

  // ========================================================================
  // Schema Merging and Type Coercion
  // ========================================================================

  @Test
  public void testUnionWithDifferentIndicesSchemaMerge() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| union [search source=%s | where age > 35 | fields account_number,"
                    + " firstname, balance] [search source=%s | where age > 35 | fields"
                    + " account_number, balance] | stats count() as total_count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_BANK));

    verifySchema(result, schema("total_count", null, "bigint"));
    verifyDataRows(result, rows(241L));
  }

  @Test
  public void testUnionNumericCoercion_BigIntPlusInteger() throws IOException {
    // Compatible numeric types coerce to wider type
    // BIGINT (balance) + INTEGER (eval literal) → BIGINT
    // This previously caused ClassCastException, now fixed with explicit CAST
    JSONObject result =
        executeQuery(
            String.format(
                "| union [search source=%s | where account_number = 1 | fields balance] [search"
                    + " source=%s | where account_number = 1 | eval balance = 100 | fields balance]"
                    + " | head 2",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    // Schema should be unified to BIGINT (wider type)
    verifySchema(result, schema("balance", null, "bigint"));

    // Verify we get 2 rows without ClassCastException
    assertEquals(2, result.getJSONArray("datarows").length());
  }

  @Test
  public void testUnionIncompatibleTypes_MultipleFieldConflicts() throws IOException {
    // Multiple fields with incompatible types all coerce to VARCHAR
    JSONObject result =
        executeQuery(
            String.format(
                "| union [search source=%s | where account_number = 1 | fields firstname, age,"
                    + " balance] [search source=%s | where place_id = 1001 | fields description,"
                    + " age, place_id] | head 2",
                TEST_INDEX_ACCOUNT, TEST_INDEX_LOCATIONS_TYPE_CONFLICT));

    verifySchema(
        result,
        schema("firstname", null, "string"),
        schema("age", null, "string"), // BIGINT + TEXT → VARCHAR
        schema("balance", null, "bigint"),
        schema("description", null, "string"),
        schema("place_id", null, "int"));

    assertEquals(2, result.getJSONArray("datarows").length());
  }

  @Test
  public void testUnionAllDatasetsDifferentSchemas() throws IOException {
    // All 3 datasets have completely different schemas (no overlap)
    JSONObject result =
        executeQuery(
            String.format(
                "| union [search source=%s | where account_number = 1 | fields account_number,"
                    + " balance] [search source=%s | where place_id = 1001 | fields description,"
                    + " place_id] [search source=%s | where category = \\\"A\\\" | fields category,"
                    + " value] | stats count() as total",
                TEST_INDEX_ACCOUNT,
                TEST_INDEX_LOCATIONS_TYPE_CONFLICT,
                "opensearch-sql_test_index_time_data"));

    verifySchema(result, schema("total", null, "bigint"));
    verifyDataRows(result, rows(28L)); // 1 + 1 + 26 = 28
  }

  // ========================================================================
  // Mid-Pipeline Syntax
  // ========================================================================

  @Test
  public void testUnionMidPipeline_SingleExplicitDataset() throws IOException {
    // Mid-pipeline union with upstream result as implicit first dataset
    // Query: search source=x | where y | union dataset_b
    // Equivalent to: union [search source=x | where y], dataset_b
    JSONObject result =
        executeQuery(
            String.format(
                "search source=%s | where gender = \\\"M\\\" "
                    + "| union [search source=%s | where gender = \\\"F\\\"] "
                    + "| stats count() as total",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("total", null, "bigint"));
    verifyDataRows(result, rows(1000L)); // All male + all female = 1000
  }

  @Test
  public void testUnionWithExplicitOrdering() throws IOException {
    // Union with explicit ordering for deterministic results
    // Without explicit sort, output order is not guaranteed
    JSONObject result =
        executeQuery(
            String.format(
                "| union [search source=%s | where account_number = 1 | fields account_number,"
                    + " balance] [search source=%s | where account_number = 6 | fields"
                    + " account_number, balance] | sort balance desc",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result, schema("account_number", null, "bigint"), schema("balance", null, "bigint"));

    // Verify sorted order (descending by balance)
    verifyDataRows(result, rows(1L, 39225L), rows(6L, 5686L));
  }

  // ========================================================================
  // Options and Features
  // ========================================================================

  @Test
  public void testUnionWithMaxout() throws IOException {
    String ppl =
        "| union maxout=5 "
            + "[search source=%s | where gender = \\\"M\\\"] "
            + "[search source=%s | where gender = \\\"F\\\"]";
    JSONObject result = executeQuery(String.format(ppl, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(
        result,
        schema("account_number", null, "bigint"),
        schema("firstname", null, "string"),
        schema("address", null, "string"),
        schema("balance", null, "bigint"),
        schema("gender", null, "string"),
        schema("city", null, "string"),
        schema("employer", null, "string"),
        schema("state", null, "string"),
        schema("age", null, "bigint"),
        schema("email", null, "string"),
        schema("lastname", null, "string"));

    // Should return exactly 5 rows due to maxout limit
    assertEquals(5, result.getJSONArray("datarows").length());
  }

  // ========================================================================
  // Edge Cases
  // ========================================================================

  @Test
  public void testUnionWithEmptySubsearch() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "| union "
                    + "[search source=%s | where age > 25] "
                    + "[search source=%s | where age > 200 | eval impossible = \\\"yes\\\"] "
                    + "| stats count",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("count", null, "bigint"));
    verifyDataRows(result, rows(733L));
  }

  @Test
  public void testUnionWithAllEmptyDatasets() throws IOException {
    // All datasets return zero rows
    JSONObject result =
        executeQuery(
            String.format(
                "| union "
                    + "[search source=%s | where age > 1000] "
                    + "[search source=%s | where age > 1000] "
                    + "| stats count() as total",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("total", null, "bigint"));
    verifyDataRows(result, rows(0L)); // No rows from any dataset
  }

  @Test
  public void testUnionPreservesDuplicatesExactCopy() throws IOException {
    // Duplicate rows are preserved (UNION ALL semantics)
    // Same row appears multiple times across datasets - all preserved
    JSONObject result =
        executeQuery(
            String.format(
                "| union "
                    + "[search source=%s | where account_number = 1] "
                    + "[search source=%s | where account_number = 1] "
                    + "[search source=%s | where account_number = 1] "
                    + "| stats count() as total",
                TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT, TEST_INDEX_ACCOUNT));

    verifySchema(result, schema("total", null, "bigint"));
    verifyDataRows(result, rows(3L)); // Same row appears 3 times (UNION ALL)
  }

  // ========================================================================
  // Error Handling
  // ========================================================================

  @Test
  public void testUnionWithSingleSubsearchThrowsError() {
    Exception exception =
        assertThrows(
            ResponseException.class,
            () ->
                executeQuery(
                    String.format(
                        "| union " + "[search source=%s | where age > 30]", TEST_INDEX_ACCOUNT)));

    assertTrue(
        "Error message should indicate minimum dataset requirement",
        exception.getMessage().contains("Union command requires at least two datasets"));
  }
}
