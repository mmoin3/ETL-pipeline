#!/bin/bash

if [ "$1" = "test" ]; then
	echo "Running parser test suite..."
	python -m unittest discover -s tests -v
	exit $?
fi

echo "Creating virtual environment..."
python3 -m venv venv
echo "Activating environment..."
source venv/bin/activate
echo "Installing requirements..."
pip install -r requirements.txt
echo "Setup complete!"
echo "Tip: run parser tests with './setup.sh test'."