#!/bin/bash
set -e

# stop any copies still running from a previous build
pkill -f master_app || true
pkill -f backup_app || true
pkill -f compute_app || true

echo "Building client..."
cd ./client && g++ -std=c++14 -c jobs/job.cpp -o job.o
g++ -std=c++14 -c jobs/job_loading.cpp -o job_loading.o
g++ -std=c++14 client.cpp job.o job_loading.o -o client_app -pthread
g++ -std=c++14 sample_jobs/a.cpp -o sample_jobs/a
cd ..

echo "Building primary and backup..."
cd ./master && g++ -std=c++14 -c jobs/job.cpp -o job.o
g++ -std=c++14 -c jobs/job_forward.cpp -o job_forward.o
g++ -std=c++14 master.cpp job.o job_forward.o -o master_app -lsqlite3 -pthread
g++ -std=c++14 backup.cpp job.o job_forward.o -o backup_app -lsqlite3 -pthread
cd ..

echo "Building compute node..."
cd ./compute && g++ -std=c++14 -c jobs/job.cpp -o job.o
g++ -std=c++14 -c jobs/job_execution.cpp -o job_exec.o
g++ -std=c++14 compute.cpp job.o job_exec.o -o compute_app -lsqlite3 -pthread
cd ..

echo "Done. Start each in its own terminal, in this order:"
echo "  1. cd master && ./master_app"
echo "  2. cd master && ./backup_app"
echo "  3. cd compute && ./compute_app"
echo "  4. cd client && ./client_app"
