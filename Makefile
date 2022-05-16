venv:
	PIPENV_VENV_IN_PROJECT=1 pipenv shell
	pipenv install -r requirements.txt

build:
	mkdir ./dist
	cp main.py ./dist
	cd ./src && zip -r ../dist/src.zip .

run:
	python main.py

clean:
	rm -rf dist
