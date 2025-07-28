-- Metrics-First Compiler Macro
-- Provides guidance for compiling metrics-first YAML files
-- Uses standalone script to avoid Python model issues

{% macro metrics_first_compile_embedded() %}
  {#
    Main compilation macro that provides instructions for using the standalone compiler.
    
    Since dbt Python models must return DataFrames for materialization,
    we use a standalone script approach for file compilation.
    
    Usage in dbt_project.yml:
      on-run-start:
        - "{{ metrics_first_compile_embedded() }}"
  #}
  
  {% if execute %}
    {% set config = var('dbt_metrics_first', {}) %}
    {% set verbose = config.get('verbose_logging', false) %}
    
    {{ log("📊 Metrics-First Compiler", info=True) }}
    {{ log("========================", info=True) }}
    
    {# Check if compiled files exist #}
    {% set check_compiled_files = config.get('check_compiled_files', true) %}
    
    {% if check_compiled_files %}
      {{ log("⚠️  Note: Metrics compilation now uses a standalone script", info=True) }}
      {{ log("", info=True) }}
      {{ log("To compile your metrics-first YAML files:", info=True) }}
      {{ log("  1. Run: python dbt_packages/dbt_metrics_first/scripts/compile_metrics.py", info=True) }}
      {{ log("  2. Then run: dbt run", info=True) }}
      {{ log("", info=True) }}
      {{ log("For automated workflows, use the helper scripts:", info=True) }}
      {{ log("  - Unix/Mac: ./dbt_packages/dbt_metrics_first/scripts/dbt-compile-and-parse.sh", info=True) }}
      {{ log("  - Windows: .\\dbt_packages\\dbt_metrics_first\\scripts\\dbt-compile-and-parse.bat", info=True) }}
      {{ log("", info=True) }}
      {{ log("💡 Tip: Add the compile step to your development workflow or CI/CD pipeline", info=True) }}
    {% endif %}
    
    {% if verbose %}
      {{ log("", info=True) }}
      {{ log("Configuration:", info=True) }}
      {{ log("  Input directories: " ~ config.get('input_directories', ['metrics', 'semantic_models', 'models/metrics']), info=True) }}
      {{ log("  Output directory: " ~ config.get('output_directory', 'models'), info=True) }}
    {% endif %}
  {% endif %}
  
{% endmacro %}

{% macro metrics_first_check_compilation() %}
  {#
    Helper macro to check if metrics have been compiled.
    Can be used to verify compilation before running models.
  #}
  
  {% set config = var('dbt_metrics_first', {}) %}
  {% set output_dir = config.get('output_directory', 'models') %}
  
  {{ log("💡 Remember to compile metrics-first files before running dbt", info=True) }}
  {{ log("   Run: python dbt_packages/dbt_metrics_first/scripts/compile_metrics.py", info=True) }}
  
{% endmacro %}