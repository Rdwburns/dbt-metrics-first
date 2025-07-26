# dbt-metrics-first

A **metrics-first** approach to defining dbt semantic models. Write clean, business-friendly YAML that compiles to dbt's semantic layer format.

## 🎯 Why Metrics First?

**Current dbt semantic layer:**
- Entity-first approach requires complex nested structures
- Difficult YAML syntax with steep learning curve  
- Dimensions inherited automatically through entity relationships
- Hard for business users to understand and modify

**Metrics-First-DBT:**
- ✅ **Metrics-first thinking** - Define what you want to measure directly
- ✅ **Clean, readable YAML** - Business users can understand and contribute
- ✅ **Explicit dimension control** - Specify exactly which dimensions each metric uses
- ✅ **Full dbt compatibility** - Compiles to standard dbt semantic models
- ✅ **AI-friendly syntax** - Easy for coding assistants to generate and modify

## 🎯 How Does it Work?

Instead of defining complex semantic models first, you define your business metrics in clean, intuitive YAML:

**Before (standard dbt):**
```yaml
# Complex semantic model definition
version: 2
semantic_models:
  - name: orders_model
    model: ref('fct_orders')
    entities: [...]
    dimensions: [...]
    measures: [...]
metrics:
  - name: total_revenue
    type: simple
    type_params:
      measure: revenue_measure
```

**After (metrics-first):**
```yaml
# Simple metrics-first definition
version: 1
metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    source: fct_orders
    measure:
      type: sum
      column: order_total
    dimensions:
      - name: order_date
        type: time
        grain: day
```

## 📦 Installation

### 1. Add to packages.yml

```yaml
packages:
  - git: "https://github.com/yourusername/dbt-metrics-first-embedded.git"
    revision: v1.0.0
```

### 2. Install Dependencies

```bash
dbt deps
```

**That's it!** No pip install required. The package includes everything needed.

## 🚀 Quick Start

### 1. Configure dbt_project.yml

Add the embedded compiler to your dbt hooks:

```yaml
# dbt_project.yml
on-run-start:
  - "{{ metrics_first_compile_embedded() }}"

# Optional: Custom configuration
vars:
  dbt_metrics_first:
    input_directories:
      - "metrics"
      - "semantic_models"
    output_directory: "models"
    verbose_logging: true
    validate_schema: true
```

### 2. Create Metrics Directory

```bash
mkdir metrics/
```

### 3. Define Your First Metric

Create `metrics/revenue.yml`:

```yaml
version: 1
metrics:
  - name: total_revenue
    description: "Total revenue from completed orders"
    source: fct_orders
    measure:
      type: sum
      column: order_total
      filters:
        - "order_status = 'completed'"
    dimensions:
      - name: order_date
        type: time
        grain: day
      - name: customer_segment
        type: categorical
    meta:
      owner: "finance_team"
      tier: "gold"
```

### 4. Run dbt

```bash
dbt run
```

The embedded compiler will automatically:
1. 🔍 Detect your metrics-first files
2. ✅ Validate the YAML syntax
3. 🔄 Compile to dbt semantic models
4. 📝 Place them in your models directory

## 📖 Usage Guide

### Supported Metric Types

#### Simple Metrics
```yaml
metrics:
  - name: total_orders
    description: "Total number of orders"
    source: fct_orders
    measure:
      type: count
      column: order_id
```

#### Ratio Metrics
```yaml
metrics:
  - name: conversion_rate
    description: "Session to order conversion rate"
    type: ratio
    numerator:
      source: fct_orders
      measure:
        type: count_distinct
        column: session_id
    denominator:
      source: fct_sessions
      measure:
        type: count
        column: session_id
```

#### Derived Metrics
```yaml
metrics:
  - name: revenue_per_customer
    description: "Average revenue per customer"
    type: derived
    formula: "total_revenue / customer_count"
```

### Available Measure Types

- `sum`: Sum of column values
- `count`: Count of rows
- `count_distinct`: Count of unique values
- `avg`: Average of column values
- `min`: Minimum value
- `max`: Maximum value

### Dimension Types

- `categorical`: Standard categorical dimension
- `time`: Time-based dimension with granularity

### Time Granularities

- `day`: Daily granularity
- `week`: Weekly granularity  
- `month`: Monthly granularity
- `quarter`: Quarterly granularity
- `year`: Yearly granularity

## ⚙️ Configuration

Configure the package behavior in your `dbt_project.yml`:

```yaml
vars:
  dbt_metrics_first:
    # Input directories to search for metrics-first files
    input_directories:
      - "metrics"
      - "semantic_models"
      - "models/metrics"
    
    # Output directory for compiled models
    output_directory: "models"
    
    # File naming convention
    compiled_file_suffix: "_semantic_models"
    
    # Schema validation (recommended)
    validate_schema: true
    
    # Logging settings
    verbose_logging: false
    show_compilation_details: true
    
    # Error handling
    fail_on_compilation_error: true
    fail_on_validation_error: true
```

## 🎯 Advanced Usage

### Environment-Specific Compilation

```yaml
# dbt_project.yml
on-run-start:
  - |
    {% if target.name == 'dev' %}
      {{ metrics_first_compile_embedded() }}
    {% elif target.name == 'ci' %}
      {# Validation only in CI #}
      {{ log("📋 Validating metrics-first files in CI", info=True) }}
    {% endif %}
```

### Custom Hook Integration

```yaml
# dbt_project.yml
on-run-start:
  - "{{ metrics_first_compile_embedded() }}"

on-run-end:
  - |
    {% set config = var('dbt_metrics_first', {}) %}
    {% if config.get('show_compilation_details', true) %}
      {{ log("✅ Metrics-first compilation completed", info=True) }}
    {% endif %}
```

### Conditional Compilation

```yaml
# Only compile if metrics files exist
on-run-start:
  - |
    {% set has_metrics = true %}  {# Add logic to check for metrics files #}
    {% if has_metrics %}
      {{ metrics_first_compile_embedded() }}
    {% endif %}
```

## 🔧 How It Works

The embedded package uses dbt's Python model capabilities to provide full compilation features:

1. **Detection**: Scans configured directories for YAML files with `version: 1` and `metrics:` keys
2. **Validation**: Validates metrics structure using embedded schema validation
3. **Compilation**: Transforms metrics-first syntax to standard dbt semantic models
4. **Output**: Writes compiled semantic models to the configured output directory

## 📁 File Structure

```
dbt-metrics-first-embedded/
├── dbt_project.yml          # Package configuration
├── models/
│   └── metrics_first_embedded_compiler.py  # Embedded Python compiler
├── macros/
│   └── compiler.sql         # dbt macros for compilation
├── examples/
│   └── metrics/
│       ├── revenue_metrics.yml
│       └── conversion_metrics.yml
└── README.md
```

## 🔍 Troubleshooting

### No Metrics Files Found

```
Warning: No metrics-first files found
```

**Solutions:**
1. Ensure files have `version: 1` at the top
2. Include a `metrics:` key with a list of metrics
3. Check that files are in configured `input_directories`
4. Verify YAML syntax is valid

### Compilation Errors

```
Error: Compilation failed for metrics/revenue.yml
```

**Solutions:**
1. Check required fields: `name`, `description`, `source`
2. Verify `measure` section is properly formatted
3. Ensure dimension types are valid (`categorical`, `time`)
4. Run with `verbose_logging: true` for detailed error messages

### Permission Issues

```
Error: Cannot write to models/ directory
```

**Solutions:**
1. Ensure the output directory exists and is writable
2. Run dbt from the project root directory
3. Check file permissions on the output directory

### Python Model Errors

```
Error: Python model execution failed
```

**Solutions:**
1. Ensure your dbt version supports Python models (>=1.3.0)
2. Verify Python dependencies (yaml, pandas) are available
3. Check the dbt logs for detailed Python error messages

## 📖 Examples

See the `examples/` directory for complete examples of:
- Simple metrics (sum, count, average)
- Ratio metrics (conversion rates, percentages)
- Derived metrics (calculated from other metrics)
- Multi-dimensional metrics
- Filtered metrics

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes
4. Add examples and tests
5. Submit a pull request

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🔗 Related Projects

- **[Metrics-First-DBT CLI](https://pypi.org/project/metrics-first-dbt/)**: Full-featured CLI tool
- **[dbt-metrics-first](https://github.com/yourusername/dbt-metrics-first)**: Full dbt package with CLI integration
- **[dbt Semantic Layer](https://docs.getdbt.com/docs/build/semantic-models)**: Official dbt semantic layer docs

## 🆘 Support

- **Issues**: [GitHub Issues](https://github.com/yourusername/dbt-metrics-first-embedded/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/dbt-metrics-first-embedded/discussions)

---

**Perfect for analytics engineers who want simplified metrics without external dependencies!** 🎯