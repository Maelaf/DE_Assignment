#!/bin/bash

# Wait for PostgreSQL to start
echo "Waiting for PostgreSQL to start..."
sleep 10

# Execute SQL script into PostgreSQL database
docker-compose exec -T postgres psql -U admin -d food -f sql/taxi_service.sql

echo "SQL script executed successfully."
