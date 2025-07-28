#!/bin/bash
# Helper script for Unix/Mac to compile metrics and run dbt commands
# Usage: ./dbt-compile-and-parse.sh [dbt command and args]
# Example: ./dbt-compile-and-parse.sh parse
# Example: ./dbt-compile-and-parse.sh run

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Get the dbt project root (assuming standard dbt package structure)
DBT_PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../../.." && pwd )"

echo "🔄 Compiling metrics-first files..."
echo "   Working directory: $DBT_PROJECT_ROOT"

# Run the compiler from the project root
cd "$DBT_PROJECT_ROOT"
python "$SCRIPT_DIR/compile_metrics.py" "$@"

# Check if compilation was successful
if [ $? -eq 0 ]; then
    echo "✅ Metrics compilation successful"
    
    # If dbt command was provided, run it
    if [ $# -gt 0 ]; then
        echo ""
        echo "🚀 Running: dbt $@"
        dbt "$@"
    else
        echo ""
        echo "💡 No dbt command specified. Run any dbt command now."
        echo "   Example: dbt parse"
    fi
else
    echo "❌ Metrics compilation failed"
    exit 1
fi