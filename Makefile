run:
	@docker-compose up --build

down:
	@docker-compose down

test:
	( \
	python3 -m venv ls_env; \
	. ls_env/bin/activate; \
	pip install -r requirements.txt; \
	py.test -v tests \
	)

clean:
	rm -rf ls_env
