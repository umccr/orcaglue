# Usage:
# 	make ls
# 	make ls dd=/tmp
dd ?= workspace/

pull:
	@docker compose pull

up:
	@docker compose up --wait -d

down:
	@docker compose down

stop: down

reload: down up

ps:
	@docker compose ps

tail:
	@docker compose logs glue -f

ls:
	@docker compose exec -it glue ls -l $(dd)

pwd:
	@docker compose exec -it glue pwd

deps:
	@docker compose exec -it glue python3 -m pip install -r workspace/requirements.txt

python:
	@docker compose exec -it glue python3 -V

# just go into glue instance without (auto) installing python dependencies
in:
	@docker compose exec -it glue bash

glue: deps
	@docker compose exec -it glue bash

restart: down up glue

spark:
	@docker compose exec -it glue bash -c "/usr/local/bin/pyspark --version"

pyspark:
	@docker compose exec -it glue bash -c "/usr/local/bin/pyspark"

# run this debug target to do preflight check your AWS access
debug:
	@echo "------ get-caller-identity ------"
	aws sts get-caller-identity
	@echo "------ lz-bucket ------"
	aws s3 ls s3://orcahouse-dev-landing-zone-115253169271-ap-southeast-2-an
	@echo "------ redshift-workgroup ------"
	aws --no-cli-pager redshift-serverless get-workgroup --workgroup-name orcahouse-dev --query "workgroup.workgroupArn"
	@echo "------------"
