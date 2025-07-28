# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is **dbt-metrics-first**, a dbt package that provides a metrics-first approach to defining dbt semantic models. It allows users to write clean, business-friendly YAML that compiles to dbt's semantic layer format without requiring external dependencies.

## Key Commands

### Installation and Setup
```bash
# Install dbt dependencies (after adding to packages.yml)
dbt deps

# Run dbt with metrics compilation
dbt run
```

### Development Commands
```bash
# Run dbt with verbose logging for debugging
dbt run --vars '{"dbt_metrics_first": {"verbose_logging": true}}'

# Test the compilation without running models
dbt run --models metrics_first_embedded_compiler
```

## Architecture

### Core Components

1. **Embedded Python Compiler** (`models/metrics_first_embedded_compiler.py`): 
   - Main compilation logic that transforms metrics-first YAML to dbt semantic models
   - Uses only Python standard library (no external dependencies)
   - Called via dbt's Python model capability

2. **Compiler Macro** (`macros/compiler.sql`):
   - Entry point for compilation via `metrics_first_compile_embedded()` macro
   - Handles configuration and error handling
   - Integrates with dbt's on-run-start hooks

3. **Configuration** (`dbt_project.yml`):
   - Default settings for input/output directories
   - Compilation behavior options
   - Requires dbt version >= 1.3.0 (for Python model support)

### Compilation Flow

1. User adds `{{ metrics_first_compile_embedded() }}` to `on-run-start` hook
2. During `dbt run`, the macro executes the embedded Python compiler
3. Compiler scans configured directories for metrics-first YAML files (version: 1)
4. Validates and transforms metrics to dbt semantic model format
5. Writes compiled models to output directory

### Metrics-First Format

Input YAML files should have:
- `version: 1` at the top
- `metrics:` key with list of metric definitions
- Each metric must have: `name`, `description`, `source`, `measure`
- Optional: `dimensions`, `filters`, `meta`

Supported metric types:
- Simple metrics (sum, count, avg, min, max, count_distinct)
- Ratio metrics (numerator/denominator)
- Derived metrics (formula-based)
- Conversion metrics (entity-based conversion funnels with time windows)
- Cumulative metrics (rolling windows, grain-to-date, all-time accumulations)

### Configuration Variables

Set in `dbt_project.yml` under `vars.dbt_metrics_first`:
- `input_directories`: Where to find metrics YAML files
- `output_directory`: Where to write compiled semantic models
- `validate_schema`: Enable/disable YAML validation
- `verbose_logging`: Debug logging
- `fail_on_compilation_error`: Error handling behavior