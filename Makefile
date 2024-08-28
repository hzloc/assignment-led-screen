run:
	export POSTGRES_DB=example
	docker-compose up --build -d
down:
	docker-compose down -v