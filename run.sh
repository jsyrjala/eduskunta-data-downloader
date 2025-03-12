#!/bin/bash
# Wrapper script to run the main.py script with the correct Python environment

# Get the directory where this script is located
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Run the main.py script with all arguments passed to this script
python -m pip install -r "$DIR/requirements.txt" > /dev/null 2>&1
python "$DIR/main.py" "$@"