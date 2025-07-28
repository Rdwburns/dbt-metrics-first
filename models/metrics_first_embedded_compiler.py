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
                    dimension_def = {
                        'name': dim_name,
                        'type': dim.get('type', 'categorical'),
                        'time_granularity': dim.get('grain', 'day') if dim.get('type') == 'time' else None
                    }
                    
                    # Add expr if provided
                    if 'expr' in dim:
                        dimension_def['expr'] = dim['expr']
                    
                    # Add label if provided
                    if 'label' in dim:
                        dimension_def['label'] = dim['label']
                    
                    dimensions.append(dimension_def)
                    seen_dimensions.add(dim_name)
            
            # Add entities
            for entity in metric.get('entities', []):
                entity_name = entity['name']
                if entity_name not in seen_entities:
                    entity_def = {
                        'name': entity_name,
                        'type': entity.get('type', 'primary')
                    }
                    
                    # Add expr if provided (for composite keys)
                    if 'expr' in entity:
                        entity_def['expr'] = entity['expr']
                    
                    entities.append(entity_def)
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
                    
                    # Add non-additive dimension if present
                    if 'non_additive_dimension' in metric['measure']:
                        measure['non_additive_dimension'] = metric['measure']['non_additive_dimension']
                    
                    measures.append(measure)
                    seen_measures.add(measure_name)
            
            # Handle measures for ratio metrics
            metric_type = metric.get('type', 'simple')
            if metric_type == 'ratio':
                # Add numerator measure
                if 'numerator' in metric and 'measure' in metric['numerator']:
                    num_measure_name = f"{metric['numerator']['name']}_measure"
                    if num_measure_name not in seen_measures:
                        num_measure = {
                            'name': num_measure_name,
                            'agg': metric['numerator']['measure'].get('type', 'sum'),
                            'expr': metric['numerator']['measure'].get('column', metric['numerator']['name'])
                        }
                        if 'filters' in metric['numerator']['measure']:
                            num_measure['agg_params'] = {
                                'where': ' AND '.join(metric['numerator']['measure']['filters'])
                            }
                        if 'non_additive_dimension' in metric['numerator']['measure']:
                            num_measure['non_additive_dimension'] = metric['numerator']['measure']['non_additive_dimension']
                        measures.append(num_measure)
                        seen_measures.add(num_measure_name)
                
                # Add denominator measure
                if 'denominator' in metric and 'measure' in metric['denominator']:
                    den_measure_name = f"{metric['denominator']['name']}_measure"
                    if den_measure_name not in seen_measures:
                        den_measure = {
                            'name': den_measure_name,
                            'agg': metric['denominator']['measure'].get('type', 'sum'),
                            'expr': metric['denominator']['measure'].get('column', metric['denominator']['name'])
                        }
                        if 'filters' in metric['denominator']['measure']:
                            den_measure['agg_params'] = {
                                'where': ' AND '.join(metric['denominator']['measure']['filters'])
                            }
                        if 'non_additive_dimension' in metric['denominator']['measure']:
                            den_measure['non_additive_dimension'] = metric['denominator']['measure']['non_additive_dimension']
                        measures.append(den_measure)
                        seen_measures.add(den_measure_name)
            
            # Handle measures for cumulative metrics
            elif metric_type == 'cumulative':
                if 'measure' in metric and 'measure' in metric['measure']:
                    cum_measure_name = f"{metric['measure']['name']}_measure"
                    if cum_measure_name not in seen_measures:
                        cum_measure = {
                            'name': cum_measure_name,
                            'agg': metric['measure']['measure'].get('type', 'sum'),
                            'expr': metric['measure']['measure'].get('column', metric['measure']['name'])
                        }
                        if 'filters' in metric['measure']['measure']:
                            cum_measure['agg_params'] = {
                                'where': ' AND '.join(metric['measure']['measure']['filters'])
                            }
                        if 'non_additive_dimension' in metric['measure']['measure']:
                            cum_measure['non_additive_dimension'] = metric['measure']['measure']['non_additive_dimension']
                        measures.append(cum_measure)
                        seen_measures.add(cum_measure_name)
        
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
        
        # Build base metric structure with common parameters
        base_metric = {
            'name': metric['name'],
            'description': metric['description'],
            'type': metric_type
        }
        
        # Add optional common parameters
        if 'label' in metric:
            base_metric['label'] = metric['label']
        
        if 'filter' in metric:
            base_metric['filter'] = metric['filter']
        
        if 'config' in metric:
            base_metric['config'] = metric['config']
        
        if 'meta' in metric:
            base_metric['meta'] = metric['meta']
        
        if 'offset_window' in metric:
            base_metric['offset_window'] = metric['offset_window']
        
        if 'fill_nulls_with' in metric:
            base_metric['fill_nulls_with'] = metric['fill_nulls_with']
        
        if metric_type == 'simple':
            base_metric['type_params'] = {
                'measure': f"{metric['name']}_measure"
            }
            return base_metric
        
        elif metric_type == 'ratio':
            base_metric['type_params'] = {
                'numerator': f"{metric['numerator']['name']}_measure",
                'denominator': f"{metric['denominator']['name']}_measure"
            }
            return base_metric
        
        elif metric_type == 'derived':
            base_metric['type_params'] = {
                'expr': metric['formula']
            }
            return base_metric
        
        elif metric_type == 'conversion':
            # Build conversion metric
            conversion_params = {
                'conversion_type_params': {
                    'entity': metric['entity']
                }
            }
            
            # Add base measure
            if 'base_measure' in metric:
                conversion_params['conversion_type_params']['base_measure'] = {
                    'name': f"{metric['base_measure']['name']}_measure"
                }
            
            # Add conversion measure
            if 'conversion_measure' in metric:
                conversion_params['conversion_type_params']['conversion_measure'] = {
                    'name': f"{metric['conversion_measure']['name']}_measure"
                }
            
            # Add optional window
            if 'window' in metric:
                conversion_params['conversion_type_params']['window'] = metric['window']
            
            # Add optional calculation type
            if 'calculation' in metric:
                conversion_params['conversion_type_params']['calculation'] = metric['calculation']
            
            # Add optional constant properties
            if 'constant_properties' in metric:
                conversion_params['conversion_type_params']['constant_properties'] = metric['constant_properties']
            
            base_metric['type_params'] = conversion_params
            return base_metric
        
        elif metric_type == 'cumulative':
            # Build cumulative metric
            cumulative_params = {
                'measure': f"{metric['measure']['name']}_measure"
            }
            
            # Add optional window
            if 'window' in metric:
                cumulative_params['window'] = metric['window']
            
            # Add optional grain_to_date
            if 'grain_to_date' in metric:
                cumulative_params['grain_to_date'] = metric['grain_to_date']
            
            base_metric['type_params'] = cumulative_params
            return base_metric
        
        else:
            # Default to simple for unknown types
            base_metric['type'] = 'simple'
            base_metric['type_params'] = {
                'measure': f"{metric['name']}_measure"
            }
            return base_metric
    
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