# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

OpenSearch SQL plugin enables querying OpenSearch using SQL and Piped Processing Language (PPL). The project has evolved through multiple engine versions (V2 and V3), with V3 integrating Apache Calcite for improved query optimization and broader SQL feature support.

## Build and Test Commands

### Build Commands
- `./gradlew build` - Full build with all tests (takes time)
- `./gradlew build -x integTest` - Faster build skipping integration tests
- `./gradlew assemble` - Generate jar and zip files in build/distributions
- `./gradlew spotlessApply` - Auto-format code using Spotless/Google Java Format
- `./gradlew spotlessCheck` - Check code formatting

### Testing Commands

**Unit Tests** (files ending with `*Test.java` in module `src/test/java/`):
- `./gradlew test` - Run all unit tests
- `./gradlew :<module_name>:test` - Run tests for specific module (e.g., `:core:test`)

**Integration Tests** (files ending with `*IT.java` in `integ-test/` module):
- `./gradlew :integ-test:integTest` - Run all integration tests
- `./gradlew :integ-test:integTest -Dtests.class="*QueryIT"` - Run specific test class (old syntax)
- `./gradlew :integ-test:integTest --tests "org.opensearch.sql.calcite.remote.CalcitePPLAggregationIT"` - Run specific test class (preferred)
- `./gradlew :integ-test:integTest --tests "*.CalcitePPLAggregationIT.testEarliestAndLatest"` - Run specific test method
- `./gradlew :integ-test:integTest --tests "*.CalcitePPLAggregationIT.testEarliest*"` - Run tests matching pattern
- `./gradlew :integ-test:integTest --tests "TestClass.testMethod" --info` - Run with verbose output for debugging

**Other Test Commands**:
- `./gradlew :integ-test:yamlRestTest` - Run REST integration tests
- `./gradlew :doctest:doctest` - Run documentation tests
- `./gradlew :doctest:doctest -Pdocs=search,fields` - Run specific doc tests
- `./gradlew pitest` - Run PiTest mutation testing

**Integration Test Debugging**:
- Server-side logs location: `integ-test/build/testclusters/integTest-0/logs/`
- Remote cluster logs: `integ-test/build/testclusters/remoteCluster-0/logs/`
- Test data files: `integ-test/src/test/resources/`

**Integration Test Debugging Workflow**:
1. Run failing test in isolation using `--tests` flag
2. Enable verbose logging with `--info` flag
3. Examine test data in `integ-test/src/test/resources/`
4. Manually verify expected results against test data
5. Check server-side logs for production code output (not visible in test logs)
6. Fix incrementally: one test at a time, verify, then move to next

### Prometheus-Related Flags
- Add `-DignorePrometheus` to skip Prometheus setup when not available (e.g., `./gradlew :integ-test:integTest -DignorePrometheus`)

### Other Commands
- `./gradlew generateGrammarSource` - Regenerate ANTLR parser from grammar files
- `./gradlew :opensearch-sql-plugin:run` - Quick start OpenSearch with plugin installed
- `./gradlew :opensearch-sql-plugin:run -DdebugJVM` - Start with remote debugging on port 5005

## Architecture Overview

### Module Structure (Gradle subprojects)
- **plugin** - OpenSearch plugin integration and REST APIs
- **sql** - SQL language parser and processor
- **ppl** - PPL (Piped Processing Language) parser and processor
- **core** - Query engine core (analyzer, planner, executor, expression framework)
- **opensearch** - OpenSearch storage engine and DSL translation
- **prometheus** - Prometheus data source connector
- **protocol** - Request/response protocol formatters (JDBC, CSV, etc.)
- **common** - Shared utilities
- **api** - Public API interfaces
- **datasources** - Multi-datasource support
- **async-query-core** / **async-query** - Async query execution (Spark integration)
- **direct-query-core** / **direct-query** - Direct query support
- **language-grammar** - ANTLR grammar definitions
- **integ-test** - Integration and comparison tests
- **doctest** - Documentation tests using Python doctest library
- **legacy** - Legacy code from NLPchina/elasticsearch-sql

### Key Package Organization
- **Core**: `org.opensearch.sql.{ast.tree, expression, calcite, executor, common}`
- **PPL**: `org.opensearch.sql.ppl.{antlr, parser, utils, calcite}`
- **OpenSearch**: `org.opensearch.sql.opensearch.{executor, storage, planner, client}`
- **Generated ANTLR code**: `**/antlr/parser/**` (excluded from coverage)

### Query Engine Evolution

**V2 Engine** (current default for most queries):
- Custom analyzer, planner, and optimizer
- SQL and PPL support through shared execution engine
- Supports: window functions, subqueries, complex expressions, semi-structured data
- Limitations: Nested field queries, JOINs, some OpenSearch functions trigger fallback to legacy engine

**V3 Engine** (Apache Calcite integration):
- Integrates Apache Calcite for SQL parsing, validation, and optimization
- Provides mature RBO/CBO optimizers
- Architecture: `PPL/SQL -> ANTLR -> AST -> RelNode (Calcite) -> EnumerableRel -> OpenSearchEnumerableRel -> OpenSearch API`
- Uses RelBuilder to convert PPL AST to Calcite relational algebra
- Enables broader SQL feature support and better query optimization

### Query Processing Flow

1. **Parsing**: ANTLR-based parsers (SQL/PPL) generate AST
2. **Analysis**: Semantic analyzer validates query correctness and type compatibility
3. **Planning**: Logical plan generation and optimization (V2 uses custom planner, V3 uses Calcite)
4. **Execution**: Physical plan executed against OpenSearch via storage engine
5. **Response Formatting**: Results formatted as JDBC, CSV, or raw JSON

### Key Abstractions

- **ExecutionEngine** (`core/src/main/java/org/opensearch/sql/executor/ExecutionEngine.java`) - Query execution interface
- **StorageEngine** (`core/src/main/java/org/opensearch/sql/storage/StorageEngine.java`) - Data source abstraction
- **PhysicalPlan** - Executable query plan operators (in `core/src/main/java/org/opensearch/sql/planner/physical`)
- **Expression** - Expression evaluation framework (in `core/src/main/java/org/opensearch/sql/expression`)

### Module Dependencies & Build Order
```
┌─────────────┐    ┌─────────────┐
│     PPL     │    │     SQL     │
│   Module    │    │   Module    │
└──────┬──────┘    └──────┬──────┘
       │                  │
       └────────┬─────────┘
                │
        ┌───────▼───────┐
        │     Core      │ ← Built first (foundation)
        │  (AST, Expr,  │
        │   Calcite)    │
        └───────┬───────┘
                │
        ┌───────▼───────┐
        │  OpenSearch   │ ← Execution engine
        │    Module     │
        └───────────────┘
```

**Build order**: Core → PPL/SQL (parallel) → OpenSearch → Integration Tests

## Development Guidelines

### Core Principles
- **Prefer simplicity** - Choose simpler solutions unless there's significant functional/performance impact
- **Write self-descriptive code** - Avoid redundant comments; code should explain itself through clear naming
- **Keep it concise** - Avoid repetitive or redundant code patterns
- **Test thoroughly** - Run relevant tests after changes; don't leave failing tests unfixed
- **Comments are last resort** - Use comments only when logic isn't self-evident; explain WHY, not WHAT

### Java Requirements
- **Java 21 required** for both development and runtime
- Set `JAVA_HOME` environment variable to JDK 21 installation path
- Use SDKMAN for JDK management: `sdk install java 21.0.8-amzn`

### Code Style
- Code formatted using Spotless with Google Java Format
- Max line length: 100 characters (except imports)
- 2-space indentation
- License header required: `/* Copyright OpenSearch Contributors SPDX-License-Identifier: Apache-2.0 */`
- Run `./gradlew spotlessApply` before committing
- Naming: Classes=PascalCase, methods/variables=camelCase, constants=UPPER_SNAKE_CASE

### Clean Code Practices
- Keep methods concise (single responsibility, typically under 20 lines)
- Use meaningful names that express intent (e.g., `calculateTotalPrice()` not `calc()`)
- Prefer `Optional<T>` for nullable returns instead of null
- Use specific exception types with meaningful error messages
- Separate concerns: parsing, execution, storage in different packages

### Test Coverage
- JaCoCo reports available in `<module>/build/reports/jacoco` after running tests
- Minimum coverage requirement: 50%
- Exclude ANTLR-generated code from coverage: `**/antlr/parser/**`

### Integration Tests
- Use OpenSearch test framework with in-memory cluster
- Test classes extend `SQLIntegTestCase` or similar base classes
- Load test data with `loadIndex(Index.ACCOUNT)` in `init()` method
- Comparison test framework compares query results with other databases

### Documentation
- Reference manual in `docs/user/` (ReStructuredText format)
- Development docs in `docs/dev/` (Markdown format)
- Doc tests (`doctest/`) ensure documentation examples are executable and correct
- Major features should include design documents in `docs/dev/`

## Calcite Integration (V3 Engine)

### Key Integration Points
- **AST → RelNode Conversion**: `CalciteRelNodeVisitor` converts OpenSearch AST to Calcite logical plan
- **Expression Conversion**: `CalciteRexNodeVisitor` converts expressions to Calcite RexNode
- **Query Flow**: `AST → CalciteRelNodeVisitor → RelNode → Optimization → Physical Plan`
- **Plan Context**: `CalcitePlanContext` manages cluster, schema, type factory, and RexBuilder

### Visitor Patterns
```java
// RelNode visitor for logical operators
@Override
public RelNode visitFilter(Filter node, CalcitePlanContext context) {
    RelNode input = visit(node.getChild(), context);
    RexNode condition = visitExpression(node.getCondition(), context);
    return LogicalFilter.create(input, condition);
}

// RexNode visitor for expressions
@Override
public RexNode visitFunction(Function node, CalcitePlanContext context) {
    SqlOperator operator = context.getOperatorTable().lookupOperatorOverloads(...).get(0);
    List<RexNode> operands = node.getArguments().stream()
        .map(arg -> visit(arg, context))
        .collect(Collectors.toList());
    return context.getRexBuilder().makeCall(operator, operands);
}
```

### Optimization Rules
- **Built-in**: FilterPushdownRule, ProjectMergeRule, AggregateReduceRule, JoinReorderRule
- **Custom**: `OpenSearchFilterPushdownRule`, `OpenSearchProjectPushdownRule` for OpenSearch-specific optimizations
- **Configuration**: Rules registered in `OpenSearchCalciteConfig.createPlanner()`

## PPL Command Development Checklist

When adding a new PPL command, follow these steps:

### 1. Prerequisite
- Open RFC issue describing purpose, syntax, usage, and examples
- Obtain PM or repository maintainer approval

### 2. Grammar & AST
- Add keywords to `OpenSearchPPLLexer.g4`
- Add grammar rules to `OpenSearchPPLParser.g4`
- Create AST node class in `org.opensearch.sql.ast.tree` (prefer reusing `Argument` over new expression nodes)
- Update `commandName` and `keywordsCanBeId` rules

### 3. Visitor Pattern
- Add `visit*` method in `AbstractNodeVisitor`
- Override in: `Analyzer`, `CalciteRelNodeVisitor`, `PPLQueryDataAnonymizer`

### 4. Testing (Required)
- **Unit Tests**: Extend `CalcitePPLAbstractTest` with minimal queries, include `verifyLogical()` and `verifyPPLToSparkSQL()`
- **Integration Tests (pushdown)**: Extend `PPLIntegTestCase` with real-world queries, include `verifySchema()` and `verifyDataRows()`
- **Integration Tests (non-pushdown)**: Add test to `CalciteNoPushdownIT`
- **Explain Tests**: Add to `ExplainIT` or `CalciteExplainIT`
- **V2 Unsupported Test**: Add test in `NewAddedCommandsIT`
- **Anonymizer Tests**: Add test in `PPLQueryDataAnonymizerTest`

### 5. Documentation
- Add `.rst` file under `docs/user/ppl/cmd`
- Link new doc to `docs/user/ppl/index.rst`

## Function Implementation Pattern

When adding new functions to SQL/PPL:

1. **Add to Function Name Enum**: Add entry to `BuiltinFunctionName` enum in core module
2. **Implement Function**: Add implementation in appropriate function table:
   - `PPLFuncImpTable` for PPL-specific functions
   - SQL function tables for SQL-specific functions
3. **Consider Compatibility**: Ensure function works in both SQL and PPL contexts when applicable
4. **Test Coverage**: Create comprehensive unit and integration tests
5. **Document Behavior**: Add function documentation with parameters, return types, and examples

## Backward Compatibility

When making changes, always consider:

- **API Compatibility**: Maintain compatibility for public interfaces
- **Breaking Changes**: Document all breaking changes in release notes and provide migration paths
- **Deprecated Functionality**: Provide clear migration paths for deprecated features
- **OpenSearch Version Support**: Consider impact on existing OpenSearch installations and version compatibility matrix

## Common Troubleshooting

### Gradle Build Issues
- If integration tests hang, check for stale OpenSearch processes: `ps aux | grep -i opensearch`
- Kill hung processes and clean rebuild: `./gradlew stop && ./gradlew clean build`
- Check for multiple Gradle daemons: `ps aux | grep -i gradle` and `./gradlew stop` if needed

### Grammar Changes
- After modifying ANTLR grammar files in `language-grammar/` or `legacy/src/main/antlr/`, run `./gradlew generateGrammarSource`
- ANTLR-generated parser code is in `**/antlr/parser/**` directories

## Rio CI/CD Configuration
- Rio pipeline defined in `rio.pkl`
- Standard command: `clean precommit test`
- Main branch for PRs: `main-apple`