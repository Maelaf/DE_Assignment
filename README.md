# DE_Assignment
Data Engineering Assignment with Pyspark and Data Quality Tools
# DE_Assignment
Data Engineering Assignment : Data Quality Check



<!-- ![first display image](images/first.png)
![second display image](images/second.png) --> 

This is the second task in the Challenge, In this challenge I have tried to integrate an opensource data quality check tool called great expectations

## Setup GUIDE

- Clone the project to your local machine
    - `git clone https://github.com/Maelaf/DE_Assignment/tree/part2`

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

