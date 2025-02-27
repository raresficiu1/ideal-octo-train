# Interview for Yagro

This solution has 2 parts. First is the main notebook in which I processed the data to get to the required solution, while the second part is the docker compose to run Pyspark, Jupyter Notebook, MongoDB and Airflow together with all the code to trigger a spark submit command and to load the data to MongoDB.

**Disclaimer: The docker compose attached is entirely my work but it was put together for a similar challenge around Dec 2024. This time, however, I have sorted out a connectivity bug between the containers.**

## Main Notebook

Can be found under **/notebooks/yagro_interview.ipynb**

### First I read the data
Notice it is in csv format instead of xlsx, this was because xlsx is not natively supported by spark. Saved it in CSV format using Excel and copied it into the docker container.

If I was running locally I would've installed the required packages and loaded it from xlsx format directly. ('com.crealytics.spark.excel')

![image](https://github.com/user-attachments/assets/91758ed4-5b9d-4dea-afb4-2c5368476812)

### Process the columns into something usable

First I took the main columns from the first row and then I linked the subcolumns from the second row to each main column.
![image](https://github.com/user-attachments/assets/c030d3c2-78e3-4709-bda7-a906b37f6e4b)

Then I added an index and used it to remove the first 3 and the last rows from the original dataframe.
![image](https://github.com/user-attachments/assets/011e0833-ddae-4e63-97b9-ad75b892108f)

Finally, I turned the columns in 3 main columns + the primary key (order_id)
![image](https://github.com/user-attachments/assets/ffcec52c-b236-4d44-be58-c0fed9c7467f)

Query test!

### Exercises
![image](https://github.com/user-attachments/assets/e3e944f2-e51b-4ab5-9a71-8e4afd0bf097)

### Exercises
#### How much fuel was ordered by farms?
Compared the total columns, last row value with the actual data in the table to make sure there are no shenanigans.
![image](https://github.com/user-attachments/assets/5f924f1d-0c11-4025-ad63-50ba0f460630)

#### How much fuel was ordered by retail customers?
![image](https://github.com/user-attachments/assets/48e342b1-8672-4c3c-b830-5bbbee04c321)

#### How much fuel was ordered for same-day delivery?
Similarly added all the columns and then joined with a "notes".
![image](https://github.com/user-attachments/assets/e3081908-715e-41b8-ab5f-e65bfa6b4971)

#### How much fuel was ordered in 2012? / USA Client in 2024
Created 2 UDF functions and used them to create 2 new columns.
![image](https://github.com/user-attachments/assets/ece0ffcb-5158-46bb-afa6-1d08458e1bac)

Using a filter on year:

![image](https://github.com/user-attachments/assets/e7e836f9-d15b-43e2-8db5-f783d4a20d9a)

Using a filter on both `year` and `country` columns:
![image](https://github.com/user-attachments/assets/bdeb2117-74d6-472c-8242-a58d71b32af9)

### Storage

Wrote a test to make sure there are no null values in `order_id` column and then wrote the whole dataset (the one without any aggregations) in parquet format.

![image](https://github.com/user-attachments/assets/f676b72b-bb69-4264-8bd4-da273b1f8362)

![image](https://github.com/user-attachments/assets/cc5dcacd-3362-46ff-bffe-83d0644be55f)

I wrote the whole dataset instead of the ones with the aggregations because everything else can be generated based on the main table in a data visualization tool like Tableau.


## Docker compose and how to run everything locally!
First clone the repo locally

![image](https://github.com/user-attachments/assets/f9745bbf-869d-4753-9b8f-6406e93dbf9b)

`docker compose up` magic to create all the containers - Airflow, MongoDB, Pyspark cluster, Jupyter Notebook
![image](https://github.com/user-attachments/assets/b457e57d-2985-4c29-b511-e553e0f85144)

#### To access Jupyter Notebook
![image](https://github.com/user-attachments/assets/d660ffcc-a4aa-4989-93a0-7557c78ab48c)

#### To Access MongoDB - `mongodb://localhost:27017/` from Compass
![image](https://github.com/user-attachments/assets/36b0f4d3-e9e4-4ea2-9e2e-1fff09a33f1f)

![image](https://github.com/user-attachments/assets/9ef48c3d-d691-4454-aff7-f5013336913e)

#### To Access Airflow - `localhost:8080` and to login use airflow:airflow 
![image](https://github.com/user-attachments/assets/f153e761-05f4-4082-9b9c-61a7496b5b6f)

### Triggering the Airflow DAG

#### First a short explanation on how this works

Inside the notebooks folder there are 2 more files, a notebook which contains the stripped code and the .py file that can be triggered using a `spark submit` command

![image](https://github.com/user-attachments/assets/0e814500-aece-4bbd-a3e2-39ea664051db)

This `spark submit` command is triggered by the Airflow dag present under the dags folder
![image](https://github.com/user-attachments/assets/90ed6de1-732b-465b-8af4-79377cbd2664)

`nano final.py`

![image](https://github.com/user-attachments/assets/4666a494-78d3-42d0-ac6a-f4af12b04ff1)

The reason inside the command is `/home/jovyan/work/final.py` because it is actually executed from inside the `pyspark` docker container which is mapped to an external path.


#### Running it from airflow
Once logged in, select the active tab (or search for final_dag) and then run it

![image](https://github.com/user-attachments/assets/beaa20e1-782e-4369-849b-92aacd9b4b83)

![image](https://github.com/user-attachments/assets/ec8b69ab-9f32-48d1-b009-d47e4699502c)

#### Once its complete the data will be visible from MongoDB
![image](https://github.com/user-attachments/assets/00ce6450-bc4a-4309-9540-2265eec53f78)

#### If it fails!
`ls -al /var/run/docker.sock` -- Make sure the owner is the same owner as the one running `docker compose up`

If it is `root`,then run `sudo chown <user_id>:docker /var/run/docker.sock`

![image](https://github.com/user-attachments/assets/e214d449-77d3-4bd6-8220-067ac4ba2eb3)

`sudo chmod 660 /var/run/docker.sock` might also be required

