@echo off
if /I "%~1"=="test" (
	echo Running parser test suite...
	python -m unittest discover -s tests -v
	exit /b %ERRORLEVEL%
)

echo Creating virtual environment...
python -m venv venv
echo Activating environment...
call venv\Scripts\activate
echo Installing requirements...
pip install -r requirements.txt
echo Setup complete! Use 'venv\Scripts\activate' to start working.
echo Tip: run parser tests with 'setup.bat test'.
pause