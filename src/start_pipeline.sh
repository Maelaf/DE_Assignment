#!/bin/bash

# Start Docker Compose
docker-compose up -d

# Wait for Docker services to start (adjust as needed)
sleep 10

# Run spark_script.py with uber_data.csv
echo "Running spark_script.py with uber_data.csv..."
spark-submit spark_script.py data/uber_data.csv

echo "Logging into MongoDB database..."
mongo_uri="mongodb://localhost:27017/"
mongo_database="uber_data_db"
mongo_collection="uber_data_collection"
mongo_query="db = client['$mongo_database']; collection = db['$mongo_collection']; documents = collection.find_one(); for document in documents: print(document); client.close()"

# Run the MongoDB query
mongo --eval "$mongo_query"


# Run spark_script.py with uber_change.csv
echo "Running spark_script.py with uber_change.csv..."
spark-submit spark_script.py data/uber_change.csv

# Run the MongoDB query again
mongo --eval "$mongo_query"

# Stop Docker Compose after processing
docker-compose down



# chmod +x start_pipeline.sh