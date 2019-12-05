# Dev env name
DEV_ENV:=livy-submit-dev
# Shell that make should use
SHELL:=bash

help:
# http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

clean: ## Remove dev environment
	conda remove -n $(DEV_ENV)
	cd docs && make clean
dev: ## Make dev environment locally
	conda create -n $(DEV_ENV) --file requirements.txt --file requirements-dev.txt -c conda-forge -y
	source activate $(DEV_ENV) && \
	    pip install -e . && \
    	    python -m ipykernel install --user --name livy-submit-dev

docs: dev ## Make docs conda environment and build the docs
	source activate $(DEV_ENV) && \
		conda install --file requirements-docs.txt -c conda-forge -y && \
		pushd docs && make html
	
	
