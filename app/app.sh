#!/bin/bash
# Start ssh server
service ssh restart 

# Starting the services
bash start-services.sh

# Creating a virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install any packages
pip install -r requirements.txt  

# Package the virtual env.
venv-pack -o .venv.tar.gz

# Collect data
# Change a.parquet to the name of your parquet file
bash prepare_data.sh a.parquet


# Run the indexer
bash index.sh

chmod +x search.sh
# Run the ranker
bash search.sh "how to build nuclear bomb?"