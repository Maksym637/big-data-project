# run application for Linux users
run-app-linux:
	cd / && python3 $(shell pwd)/main.py

# run application for Windows users
run-app-windows:
	python main.py
