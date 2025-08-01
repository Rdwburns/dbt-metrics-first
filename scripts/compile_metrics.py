#!/usr/bin/env python3
"""
Standalone Metrics-First Compiler for dbt
=========================================

This script compiles metrics-first YAML files to dbt semantic models.
It can be run independently of dbt to compile metrics before parsing.

Usage:
    python compile_metrics.py [--verbose] [--input-dir DIRS] [--output-dir DIR]
"""

import os
import sys
import json
import yaml
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional


# Custom YAML representers for better formatting
def str_presenter(dumper, data):
    """Custom string presenter that handles multiline strings properly."""
    if len(data.splitlines()) > 1:  # check for multiline string
        return dumper.represent_scalar('tag:yaml.org,2002:str', data, style='|')
    return dumper.represent_scalar('tag:yaml.org,2002:str', data)


def setup_yaml():
    """Setup custom YAML formatting."""
    yaml.add_representer(str, str_presenter)
    # Prevent unicode escape sequences
    yaml.SafeDumper.yaml_representers[None] = lambda self, data: \
        self.represent_str(str(data))


# Supported aggregation types in dbt semantic layer
SUPPORTED_AGGREGATIONS = [
    'sum', 'count', 'count_distinct', 'average', 'min', 'max',
    'median', 'percentile', 'sum_boolean'
]

# Mapping for common abbreviations to correct aggregation types
AGGREGATION_ALIASES = {
    'avg': 'average',
    'cnt': 'count',
    'cnt_distinct': 'count_distinct',
    'count_unique': 'count_distinct'
}


class MetricsCompiler:
    """
    Standalone metrics compiler that transforms metrics-first YAML to dbt semantic models.
    """
    
    def __init__(self, input_directories: List[str], output_directory: str, 
                 validate_schema: bool = True, verbose: bool = False):
        self.input_directories = input_directories
        self.output_directory = output_directory
        self.validate_schema = validate_schema
        self.verbose = verbose
        setup_yaml()
        
    def log(self, message: str, level: str = "info"):
        """Log a message if verbose mode is enabled."""
        if self.verbose or level == "error":
            prefix = {
                "info": "ℹ️ ",
                "success": "✅",
                "error": "❌",
                "warning": "⚠️ "
            }.get(level, "")
            print(f"{prefix} {message}")
    
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
            self.log("No metrics-first files found", "warning")
            self.log("Create YAML files with 'version: 1' and 'metrics:' in:", "info")
            for dir_name in self.input_directories:
                self.log(f"  - {dir_name}/", "info")
            return results
        
        self.log(f"Found {len(metrics_files)} metrics-first file(s)", "info")
        
        # Process each file
        for file_path in metrics_files:
            try:
                self.log(f"Processing {file_path}", "info")
                
                # Load and validate YAML
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                
                if self.validate_schema:
                    self._validate_metrics_schema(data, file_path)
                
                # Compile to semantic models
                semantic_models = self._compile_to_semantic_models(data, file_path)
                
                # Write output
                output_path = self._get_output_path(file_path)
                self._write_semantic_models(semantic_models, output_path)
                
                results['files_processed'] += 1
                results['models_generated'] += len(semantic_models.get('semantic_models', []))
                results['metrics_processed'] += len(data.get('metrics', []))
                
            except Exception as e:
                error_msg = f"Error processing {file_path}: {str(e)}"
                results['errors'].append(error_msg)
                self.log(error_msg, "error")
        
        # Summary
        self.log(f"Compilation completed: {results['files_processed']} files, "
                 f"{results['metrics_processed']} metrics, "
                 f"{results['models_generated']} models generated", "success")
        
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
            
            # Validate aggregation type if present
            if 'measure' in metric and 'type' in metric['measure']:
                agg_type = metric['measure']['type']
                # Check if it's an alias
                if agg_type in AGGREGATION_ALIASES:
                    agg_type = AGGREGATION_ALIASES[agg_type]
                    metric['measure']['type'] = agg_type  # Update to correct type
                    
                if agg_type not in SUPPORTED_AGGREGATIONS:
                    raise ValueError(f"Metric '{metric['name']}' has unsupported aggregation type '{agg_type}'. "
                                   f"Supported types: {', '.join(SUPPORTED_AGGREGATIONS)}")
            
            # Validate percentile parameters
            if 'measure' in metric and metric['measure'].get('type') == 'percentile':
                if 'agg_params' not in metric['measure']:
                    raise ValueError(f"Metric '{metric['name']}' with type 'percentile' requires 'agg_params'")
                if 'percentile' not in metric['measure']['agg_params']:
                    raise ValueError(f"Metric '{metric['name']}' with type 'percentile' requires 'agg_params.percentile'")
    
    def _compile_to_semantic_models(self, data: Dict, file_path: str = None) -> Dict:
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
            semantic_model = self._build_semantic_model(source, source_metrics, file_path)
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
    
    def _build_semantic_model(self, source: str, metrics: List[Dict], file_path: str = None) -> Dict:
        """Build a semantic model from a source and its metrics."""
        
        # Create unique model name using file basename if provided
        # This prevents duplicates when same source is used in multiple files
        if file_path:
            file_basename = Path(file_path).stem
            model_name = f"{source}_{file_basename}_semantic_model"
        else:
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
            # Handle dimensions - support both old string format and new dict format
            if 'dimensions' in metric:
                dims = metric['dimensions']
                for dim in dims:
                    if isinstance(dim, str):
                        # Old format: just dimension name as string
                        dim_name = dim
                        dim_dict = {
                            'name': dim_name,
                            'type': 'time' if 'date' in dim_name.lower() else 'categorical'
                        }
                        if dim_dict['type'] == 'time':
                            dim_dict['type_params'] = {'time_granularity': 'day'}
                    else:
                        # New format: dimension as dictionary
                        dim_name = dim['name']
                        dim_dict = {
                            'name': dim_name,
                            'type': dim.get('type', 'categorical')
                        }
                        if dim_dict['type'] == 'time':
                            dim_dict['type_params'] = {'time_granularity': dim.get('grain', 'day')}
                        
                        # Add optional fields
                        if 'expr' in dim:
                            dim_dict['expr'] = dim['expr']
                        if 'label' in dim:
                            dim_dict['label'] = dim['label']
                    
                    if dim_name not in seen_dimensions:
                        dimensions.append(dim_dict)
                        seen_dimensions.add(dim_name)
            
            # Add entities
            for entity in metric.get('entities', []):
                entity_name = entity['name'] if isinstance(entity, dict) else entity
                if entity_name not in seen_entities:
                    entity_def = {
                        'name': entity_name,
                        'type': entity.get('type', 'primary') if isinstance(entity, dict) else 'primary'
                    }
                    
                    # Add expr if provided (for composite keys)
                    if isinstance(entity, dict) and 'expr' in entity:
                        entity_def['expr'] = entity['expr']
                    
                    entities.append(entity_def)
                    seen_entities.add(entity_name)
            
            # Add measure from the metric
            if 'measure' in metric:
                measure_name = f"{metric['name']}_measure"
                if measure_name not in seen_measures:
                    # Get aggregation type and map aliases
                    agg_type = metric['measure'].get('type', 'sum')
                    if agg_type in AGGREGATION_ALIASES:
                        agg_type = AGGREGATION_ALIASES[agg_type]
                    
                    measure = {
                        'name': measure_name,
                        'agg': agg_type,
                        'expr': metric['measure'].get('column', metric['name'])
                    }
                    
                    # Add agg_params for percentile
                    if metric['measure'].get('type') == 'percentile' and 'agg_params' in metric['measure']:
                        measure['agg_params'] = metric['measure']['agg_params']
                    
                    # Add filters if present
                    if 'filters' in metric['measure']:
                        measure['agg_params'] = measure.get('agg_params', {})
                        measure['agg_params']['where'] = ' AND '.join(metric['measure']['filters'])
                    
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
                    # Use numerator name if provided, otherwise derive from metric name
                    num_name = metric['numerator'].get('name', f"{metric['name']}_numerator")
                    num_measure_name = f"{num_name}_measure"
                    if num_measure_name not in seen_measures:
                        # Get aggregation type and map aliases
                        agg_type = metric['numerator']['measure'].get('type', 'sum')
                        if agg_type in AGGREGATION_ALIASES:
                            agg_type = AGGREGATION_ALIASES[agg_type]
                        
                        num_measure = {
                            'name': num_measure_name,
                            'agg': agg_type,
                            'expr': metric['numerator']['measure'].get('column', num_name)
                        }
                        if 'agg_params' in metric['numerator']['measure']:
                            num_measure['agg_params'] = metric['numerator']['measure']['agg_params']
                        if 'filters' in metric['numerator']['measure']:
                            num_measure['agg_params'] = num_measure.get('agg_params', {})
                            num_measure['agg_params']['where'] = ' AND '.join(metric['numerator']['measure']['filters'])
                        if 'non_additive_dimension' in metric['numerator']['measure']:
                            num_measure['non_additive_dimension'] = metric['numerator']['measure']['non_additive_dimension']
                        measures.append(num_measure)
                        seen_measures.add(num_measure_name)
                
                # Add denominator measure
                if 'denominator' in metric and 'measure' in metric['denominator']:
                    # Use denominator name if provided, otherwise derive from metric name
                    den_name = metric['denominator'].get('name', f"{metric['name']}_denominator")
                    den_measure_name = f"{den_name}_measure"
                    if den_measure_name not in seen_measures:
                        # Get aggregation type and map aliases
                        agg_type = metric['denominator']['measure'].get('type', 'sum')
                        if agg_type in AGGREGATION_ALIASES:
                            agg_type = AGGREGATION_ALIASES[agg_type]
                        
                        den_measure = {
                            'name': den_measure_name,
                            'agg': agg_type,
                            'expr': metric['denominator']['measure'].get('column', den_name)
                        }
                        if 'agg_params' in metric['denominator']['measure']:
                            den_measure['agg_params'] = metric['denominator']['measure']['agg_params']
                        if 'filters' in metric['denominator']['measure']:
                            den_measure['agg_params'] = den_measure.get('agg_params', {})
                            den_measure['agg_params']['where'] = ' AND '.join(metric['denominator']['measure']['filters'])
                        if 'non_additive_dimension' in metric['denominator']['measure']:
                            den_measure['non_additive_dimension'] = metric['denominator']['measure']['non_additive_dimension']
                        measures.append(den_measure)
                        seen_measures.add(den_measure_name)
            
            # Handle measures for cumulative metrics
            elif metric_type == 'cumulative':
                if 'measure' in metric and 'measure' in metric['measure']:
                    # Use measure name if provided, otherwise derive from metric name
                    cum_name = metric['measure'].get('name', f"{metric['name']}_cumulative")
                    cum_measure_name = f"{cum_name}_measure"
                    if cum_measure_name not in seen_measures:
                        # Get aggregation type and map aliases
                        agg_type = metric['measure']['measure'].get('type', 'sum')
                        if agg_type in AGGREGATION_ALIASES:
                            agg_type = AGGREGATION_ALIASES[agg_type]
                        
                        cum_measure = {
                            'name': cum_measure_name,
                            'agg': agg_type,
                            'expr': metric['measure']['measure'].get('column', cum_name)
                        }
                        if 'agg_params' in metric['measure']['measure']:
                            cum_measure['agg_params'] = metric['measure']['measure']['agg_params']
                        if 'filters' in metric['measure']['measure']:
                            cum_measure['agg_params'] = cum_measure.get('agg_params', {})
                            cum_measure['agg_params']['where'] = ' AND '.join(metric['measure']['measure']['filters'])
                        if 'non_additive_dimension' in metric['measure']['measure']:
                            cum_measure['non_additive_dimension'] = metric['measure']['measure']['non_additive_dimension']
                        measures.append(cum_measure)
                        seen_measures.add(cum_measure_name)
        
        # Build the semantic model
        semantic_model = {
            'name': model_name,
            'model': f"ref('{source}')",
            'dimensions': dimensions,
            'entities': entities,
            'measures': measures
        }
        
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
            # Parse filter to ensure it has proper field name
            filter_str = metric['filter']
            # If filter doesn't contain a field name, try to infer it
            if not any(op in filter_str for op in ['=', '>', '<', '!=', 'IN', 'LIKE']):
                # Assume it's missing the field name, warn the user
                if filter_str.startswith("'"):
                    # Likely a value without field
                    self.log(f"Warning: Filter '{filter_str}' in metric '{metric['name']}' may be missing field name", "warning")
            base_metric['filter'] = filter_str
        
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
            # Use numerator/denominator names if provided, otherwise derive from metric name
            num_name = metric['numerator'].get('name', f"{metric['name']}_numerator") if 'numerator' in metric else f"{metric['name']}_numerator"
            den_name = metric['denominator'].get('name', f"{metric['name']}_denominator") if 'denominator' in metric else f"{metric['name']}_denominator"
            base_metric['type_params'] = {
                'numerator': f"{num_name}_measure",
                'denominator': f"{den_name}_measure"
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
                base_name = metric['base_measure'].get('name', f"{metric['name']}_base")
                conversion_params['conversion_type_params']['base_measure'] = {
                    'name': f"{base_name}_measure"
                }
            
            # Add conversion measure
            if 'conversion_measure' in metric:
                conv_name = metric['conversion_measure'].get('name', f"{metric['name']}_conversion")
                conversion_params['conversion_type_params']['conversion_measure'] = {
                    'name': f"{conv_name}_measure"
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
            # Use measure name if provided, otherwise derive from metric name
            cum_name = metric['measure'].get('name', f"{metric['name']}_cumulative") if 'measure' in metric else f"{metric['name']}_cumulative"
            cumulative_params = {
                'measure': f"{cum_name}_measure"
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
        
        # Preserve subdirectory structure if needed
        relative_path = None
        for input_dir in self.input_directories:
            try:
                input_dir_path = Path(input_dir).resolve()
                input_file_resolved = input_file.resolve()
                relative_path = input_file_resolved.relative_to(input_dir_path)
                if self.verbose:
                    self.log(f"Found relative path: {relative_path} from base: {input_dir}", "info")
                break
            except ValueError:
                continue
        
        if relative_path and len(relative_path.parts) > 1:
            # Maintain directory structure
            output_file = relative_path.parent / f"{base_name}_semantic_models.yml"
            if self.verbose:
                self.log(f"Preserving structure: {output_file}", "info")
        else:
            output_file = Path(f"{base_name}_semantic_models.yml")
        
        output_path = Path(self.output_directory) / output_file
        
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        return str(output_path)
    
    def _write_semantic_models(self, semantic_models: Dict, output_path: str):
        """Write compiled semantic models to output file with proper formatting."""
        
        with open(output_path, 'w', encoding='utf-8') as f:
            yaml.dump(semantic_models, f, 
                     default_flow_style=False, 
                     sort_keys=False,
                     width=1000,  # Prevent line wrapping
                     allow_unicode=True,  # Properly handle unicode characters
                     encoding='utf-8')
        
        self.log(f"Written compiled models to {output_path}", "success")


def main():
    """Main entry point for the standalone compiler."""
    
    parser = argparse.ArgumentParser(
        description='Compile metrics-first YAML files to dbt semantic models'
    )
    
    parser.add_argument(
        '--input-dir', '-i',
        nargs='+',
        default=['metrics', 'semantic_models', 'models/metrics'],
        help='Input directories to search for metrics-first files'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        default='models',
        help='Output directory for compiled semantic models'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip schema validation'
    )
    
    args = parser.parse_args()
    
    # Create compiler instance
    compiler = MetricsCompiler(
        input_directories=args.input_dir,
        output_directory=args.output_dir,
        validate_schema=not args.no_validate,
        verbose=args.verbose
    )
    
    # Run compilation
    try:
        results = compiler.compile_all()
        
        # Exit with error if there were compilation errors
        if results['errors']:
            sys.exit(1)
        
    except Exception as e:
        print(f"❌ Fatal error: {str(e)}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()