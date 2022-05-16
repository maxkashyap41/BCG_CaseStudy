venv:
	PIPENV_VENV_IN_PROJECT=1 pipenv shell

build:
	pipenv install -r requirements.txt

run:
	python main.py

clean:
	rm -rf dist
	mkdir ./dist
	cp main.py ./dist
	cd ./src && zip -r ../dist/src.zip .
