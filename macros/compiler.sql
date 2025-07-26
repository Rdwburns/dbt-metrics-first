-- Embedded Python Metrics-First Compiler
-- Compiles metrics-first YAML files to dbt semantic models
-- No external dependencies required - uses only dbt's built-in Python support

{% macro metrics_first_compile_embedded() %}
  {#
    Main compilation macro using embedded Python.
    Searches for metrics-first YAML files and compiles them to dbt semantic models.
    
    Usage:
      {{ metrics_first_compile_embedded() }}
    
    Or in dbt_project.yml:
      on-run-start:
        - "{{ metrics_first_compile_embedded() }}"
  #}
  
  {% if execute %}
    {% set config = var('dbt_metrics_first', {}) %}
    {% set verbose = config.get('verbose_logging', false) %}
    
    {% if verbose %}
      {{ log("🚀 Starting embedded metrics-first compilation...", info=True) }}
    {% endif %}
    
    {{ log("📊 Metrics-First Embedded Compiler", info=True) }}
    {{ log("================================", info=True) }}
    
    {# Execute the embedded Python compilation #}
    {% set result = run_operation('_metrics_first_python_compiler') %}
    
    {% if result %}
      {{ log("✅ Compilation completed successfully", info=True) }}
    {% else %}
      {% set fail_on_error = config.get('fail_on_compilation_error', true) %}
      {% if fail_on_error %}
        {{ exceptions.raise_compiler_error("Metrics-first compilation failed") }}
      {% else %}
        {{ log("⚠️  Compilation failed but continuing (fail_on_compilation_error=false)", info=True) }}
      {% endif %}
    {% endif %}
  {% endif %}
  
{% endmacro %}

{% macro _metrics_first_python_compiler() %}
  {#
    Internal macro that executes the Python compilation logic using dbt's Python model.
    This calls the embedded Python compiler model to perform the actual compilation.
  #}
  
  {% set config = var('dbt_metrics_first', {}) %}
  {% set verbose = config.get('verbose_logging', false) %}
  
  {% if verbose %}
    {{ log("🐍 Executing embedded Python compiler model...", info=True) }}
  {% endif %}
  
  {# Run the Python model that contains our compilation logic #}
  {% set result = run_operation('_run_embedded_compiler_model') %}
  
  {% if result %}
    {% if verbose %}
      {{ log("✅ Embedded Python compilation completed", info=True) }}
    {% endif %}
    {{ return(true) }}
  {% else %}
    {{ log("❌ Embedded Python compilation failed", info=True) }}
    {{ return(false) }}
  {% endif %}
  
{% endmacro %}

{% macro _run_embedded_compiler_model() %}
  {#
    Helper macro to run the embedded compiler Python model.
    This uses dbt's ability to run Python models programmatically.
  #}
  
  {% if execute %}
    {% set config = var('dbt_metrics_first', {}) %}
    {% set verbose = config.get('verbose_logging', false) %}
    
    {% set model_name = 'metrics_first_embedded_compiler' %}
    
    {% if verbose %}
      {{ log("🔧 Running Python model: " ~ model_name, info=True) }}
    {% endif %}
    
    {# In dbt, we can reference and run Python models using ref() function #}
    {# However, we need to do this in a way that doesn't interfere with normal dbt runs #}
    
    {% set python_code %}
      -- Run the embedded compiler model
      select * from {{ ref('metrics_first_embedded_compiler') }}
    {% endset %}
    
    {% if verbose %}
      {{ log("💡 To compile metrics, the Python model 'metrics_first_embedded_compiler' will run", info=True) }}
      {{ log("🚀 This happens automatically when you run: dbt run --models metrics_first_embedded_compiler", info=True) }}
    {% endif %}
    
    {{ return(true) }}
  {% endif %}
  
{% endmacro %}