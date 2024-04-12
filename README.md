# DE_Assignment
Data Engineering Assignment with Pyspark and Data Quality Tools
# DE_Assignment
Data Engineering Assignment with Pyspark and Data Quality Tools
# DE_Assignment
Data Engineering Assignment : Pyspark Pipeline


I have added a branch for each the three parts/tasks, each branch has 
- src folder which contains all the code
- out folder which contains


This is the First task in the Challenge, In this challenge I have tried to build a spark data pipeline that can cope with a change in schema
- Refer the Notebook for short summary on the project

## Setup GUIDE

- Clone the project to your local machine
    - `git clone https://github.com/Maelaf/DE_Assignment.git`

- Create a new Virtual enviroment with python and Install all the required python dependencies
    - `python3 -m venv spark-env`
    - `source spark-env/bin/activate`
    - `pip install -r requirements.txt` 


## Running Server and Client

- Navigate to source folder
    - `cd src`
    

- spin up your docker containers
    - `docker-compose up -d`


- Make sure the bash script is executable
    - `chmod +x start_pipeline.sh`

- Configure the database
    - `./start_pipeline.sh`
    

