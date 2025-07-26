"""
Embedded Python Compiler for Metrics-First DBT
===============================================

This Python model provides embedded compilation capabilities for metrics-first YAML files
without requiring external dependencies. It uses only Python's standard library and dbt's
built-in capabilities.

Usage:
    This model is called by the metrics_first_compile_embedded() macro.
    Do not run this model directly.
"""

import os
import json
import yaml
from pathlib import Path
from typing import Dict, List, Any, Optional


def model(dbt, session):
    """
    Main embedded compiler model for metrics-first DBT.
    
    This function is called by dbt when the model runs and performs the compilation
    of metrics-first YAML files to dbt semantic models.
    """
    
    # Get configuration from dbt variables
    config = dbt.var('dbt_metrics_first', {})
    input_dirs = config.get('input_directories', ['metrics', 'semantic_models', 'models/metrics'])
    output_dir = config.get('output_directory', 'models')
    validate_schema = config.get('validate_schema', True)
    verbose = config.get('verbose_logging', False)
    
    if verbose:
        dbt.log(f"🔍 Embedded compiler starting with config: {config}")
    
    # Initialize compiler
    compiler = EmbeddedMetricsCompiler(
        input_directories=input_dirs,
        output_directory=output_dir,
        validate_schema=validate_schema,
        verbose=verbose,
        dbt=dbt
    )
    
    # Perform compilation
    try:
        results = compiler.compile_all()
        
        if verbose:
            dbt.log(f"✅ Compilation completed. Processed {results['files_processed']} files")
        
        # Return a summary DataFrame for dbt
        import pandas as pd
        return pd.DataFrame([{
            'compilation_status': 'success',
            'files_processed': results['files_processed'],
            'models_generated': results['models_generated'],
            'metrics_processed': results['metrics_processed']
        }])
        
    except Exception as e:
        dbt.log(f"❌ Compilation failed: {str(e)}")
        
        # Return error DataFrame
        import pandas as pd
        return pd.DataFrame([{
            'compilation_status': 'error',
            'error_message': str(e),
            'files_processed': 0,
            'models_generated': 0,
            'metrics_processed': 0
        }])


class EmbeddedMetricsCompiler:
    """
    Embedded metrics compiler that uses only standard library dependencies.
    """
    
    def __init__(self, input_directories: List[str], output_directory: str, 
                 validate_schema: bool = True, verbose: bool = False, dbt=None):
        self.input_directories = input_directories
        self.output_directory = output_directory
        self.validate_schema = validate_schema
        self.verbose = verbose
        self.dbt = dbt
        
    def compile_all(self) -> Dict[str, Any]:
        """Compile all metrics-first files found in input directories."""
        
        results = {
            'files_processed': 0,
            'models_generated': 0,
            'metrics_processed': 0,
            'errors': []
        }
        
        # Find all metrics-first files
        metrics_files = self._find_metrics_files()
        
        if not metrics_files:
            if self.verbose:
                self.dbt.log("⚠️  No metrics-first files found")
                self.dbt.log("💡 Create YAML files with 'version: 1' and 'metrics:' in:")
                for dir_name in self.input_directories:
                    self.dbt.log(f"   - {dir_name}/")
            return results
        
        # Process each file
        for file_path in metrics_files:
            try:
                if self.verbose:
                    self.dbt.log(f"📄 Processing {file_path}")
                
                # Load and validate YAML
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                
                if self.validate_schema:
                    self._validate_metrics_schema(data, file_path)
                
                # Compile to semantic models
                semantic_models = self._compile_to_semantic_models(data)
                
                # Write output
                output_path = self._get_output_path(file_path)
                self._write_semantic_models(semantic_models, output_path)
                
                results['files_processed'] += 1
                results['models_generated'] += len(semantic_models.get('semantic_models', []))
                results['metrics_processed'] += len(data.get('metrics', []))
                
            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                results['errors'].append(error_msg)
                if self.dbt:
                    self.dbt.log(f"❌ {error_msg}")
        
        return results
    
    def _find_metrics_files(self) -> List[str]:
        """Find all metrics-first YAML files in input directories."""
        files = []
        
        for dir_name in self.input_directories:
            dir_path = Path(dir_name)
            if dir_path.exists():
                # Look for YAML files
                for pattern in ['*.yml', '*.yaml']:
                    for file_path in dir_path.rglob(pattern):
                        try:
                            # Check if it's a metrics-first file
                            with open(file_path, 'r', encoding='utf-8') as f:
                                data = yaml.safe_load(f)
                                if isinstance(data, dict) and 'metrics' in data and data.get('version') == 1:
                                    files.append(str(file_path))
                        except Exception:
                            # Skip files that can't be parsed
                            continue
        
        return files
    
    def _validate_metrics_schema(self, data: Dict, file_path: str):
        """Basic schema validation for metrics-first files."""
        
        if not isinstance(data, dict):
            raise ValueError(f"File must contain a YAML dictionary")
        
        if data.get('version') != 1:
            raise ValueError(f"File must have 'version: 1'")
        
        if 'metrics' not in data:
            raise ValueError(f"File must contain 'metrics' key")
        
        metrics = data['metrics']
        if not isinstance(metrics, list):
            raise ValueError(f"'metrics' must be a list")
        
        for i, metric in enumerate(metrics):
            if not isinstance(metric, dict):
                raise ValueError(f"Metric {i} must be a dictionary")
            
            if 'name' not in metric:
                raise ValueError(f"Metric {i} missing required 'name' field")
            
            if 'description' not in metric:
                raise ValueError(f"Metric {i} missing required 'description' field")
    
    def _compile_to_semantic_models(self, data: Dict) -> Dict:
        """Compile metrics-first data to dbt semantic models."""
        
        semantic_models = []
        metrics = []
        
        # Group metrics by source table
        metrics_by_source = {}
        for metric in data['metrics']:
            source = metric.get('source', 'default_source')
            if source not in metrics_by_source:
                metrics_by_source[source] = []
            metrics_by_source[source].append(metric)
        
        # Generate semantic models for each source
        for source, source_metrics in metrics_by_source.items():
            semantic_model = self._build_semantic_model(source, source_metrics)
            semantic_models.append(semantic_model)
            
            # Generate metrics definitions
            for metric in source_metrics:
                metric_def = self._build_metric_definition(metric, semantic_model['name'])
                metrics.append(metric_def)
        
        return {
            'version': 2,
            'semantic_models': semantic_models,
            'metrics': metrics
        }
    
    def _build_semantic_model(self, source: str, metrics: List[Dict]) -> Dict:
        """Build a semantic model from a source and its metrics."""
        
        model_name = f"{source}_semantic_model"
        
        # Collect all dimensions and entities from metrics
        dimensions = []
        entities = []
        measures = []
        
        # Track unique dimensions and entities
        seen_dimensions = set()
        seen_entities = set()
        seen_measures = set()
        
        for metric in metrics:
            # Add dimensions
            for dim in metric.get('dimensions', []):
                dim_name = dim['name']
                if dim_name not in seen_dimensions:
                    dimensions.append({
                        'name': dim_name,
                        'type': dim.get('type', 'categorical'),
                        'time_granularity': dim.get('grain', 'day') if dim.get('type') == 'time' else None
                    })
                    seen_dimensions.add(dim_name)
            
            # Add entities
            for entity in metric.get('entities', []):
                entity_name = entity['name']
                if entity_name not in seen_entities:
                    entities.append({
                        'name': entity_name,
                        'type': entity.get('type', 'primary')
                    })
                    seen_entities.add(entity_name)
            
            # Add measure from the metric
            if 'measure' in metric:
                measure_name = f"{metric['name']}_measure"
                if measure_name not in seen_measures:
                    measure = {
                        'name': measure_name,
                        'agg': metric['measure'].get('type', 'sum'),
                        'expr': metric['measure'].get('column', metric['name'])
                    }
                    
                    # Add filters if present
                    if 'filters' in metric['measure']:
                        measure['agg_params'] = {
                            'where': ' AND '.join(metric['measure']['filters'])
                        }
                    
                    measures.append(measure)
                    seen_measures.add(measure_name)
        
        # Build the semantic model
        semantic_model = {
            'name': model_name,
            'model': f"ref('{source}')",
            'dimensions': [d for d in dimensions if d['time_granularity'] is None] + 
                         [d for d in dimensions if d['time_granularity'] is not None],
            'entities': entities,
            'measures': measures
        }
        
        # Remove None values from time dimensions
        for dim in semantic_model['dimensions']:
            if dim.get('time_granularity') is None:
                dim.pop('time_granularity', None)
        
        return semantic_model
    
    def _build_metric_definition(self, metric: Dict, semantic_model_name: str) -> Dict:
        """Build a metric definition from metrics-first metric."""
        
        metric_type = metric.get('type', 'simple')
        
        if metric_type == 'simple':
            return {
                'name': metric['name'],
                'description': metric['description'],
                'type': 'simple',
                'type_params': {
                    'measure': f"{metric['name']}_measure"
                },
                'meta': metric.get('meta', {})
            }
        
        elif metric_type == 'ratio':
            return {
                'name': metric['name'],
                'description': metric['description'],
                'type': 'ratio',
                'type_params': {
                    'numerator': f"{metric['numerator']['name']}_measure",
                    'denominator': f"{metric['denominator']['name']}_measure"
                },
                'meta': metric.get('meta', {})
            }
        
        elif metric_type == 'derived':
            return {
                'name': metric['name'],
                'description': metric['description'],
                'type': 'derived',
                'type_params': {
                    'expr': metric['formula']
                },
                'meta': metric.get('meta', {})
            }
        
        else:
            # Default to simple for unknown types
            return {
                'name': metric['name'],
                'description': metric['description'],
                'type': 'simple',
                'type_params': {
                    'measure': f"{metric['name']}_measure"
                },
                'meta': metric.get('meta', {})
            }
    
    def _get_output_path(self, input_path: str) -> str:
        """Generate output path for compiled semantic models."""
        
        input_file = Path(input_path)
        base_name = input_file.stem
        
        output_file = f"{base_name}_semantic_models.yml"
        output_path = Path(self.output_directory) / output_file
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        return str(output_path)
    
    def _write_semantic_models(self, semantic_models: Dict, output_path: str):
        """Write compiled semantic models to output file."""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(semantic_models, f, default_flow_style=False, sort_keys=False)
        
        if self.verbose and self.dbt:
            self.dbt.log(f"✅ Written compiled models to {output_path}")