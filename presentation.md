# VBO Data Engineering Bootcamp Final Project-4: Airflow/Delta Lake

- Use this dataset: https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip

There are two different datasets in this zip file.
- tmdb_5000_credits.csv
- tmdb_5000_movies.csv
## Architecture
![](images/architecture.png)
-----
## Project Steps

- Data-generator ile MinIO `tmdb-bronze` bucket'a veri üretilmesi
- Verinin Spark ile `tmdb-bronze` dan alınıp istenen tablolara dönüştürülmesi ve `tmdb-silver`a yazılması
- Airflow ile günlük olarak tetiklenmesi

## **Connect spark_client, create virtualenv and install pip packages**
```markdown
(base) [train@10 01_airflow_spark_sqoop]$ docker-compose up -d
docker exec -it spark_client bash
cd /dataops
```

```markdown
source airflowenv/bin/activate
pip install boto3
```

## **Start jupyterlab**

```markdown
jupyter lab --ip 0.0.0.0 --port 8888 --allow-root
```
![](images/img.png)


```
(base) [train@10 01_airflow_spark_sqoop]$ docker exec -it spark_client bash

root@e9ab370b124e:/# apt update && apt install  openssh-server sudo -y
root@e9ab370b124e:/# useradd -rm -d /home/ssh_train -s /bin/bash -g root -G sudo -u 1000 ssh_train

root@e9ab370b124e:/# echo 'ssh_train:Ankara06' | chpasswd
root@e9ab370b124e:/# service ssh start

root@e9ab370b124e:/opt/spark# sudo chown -R ssh_train:1000 /opt/spark/history

```
```
root@e9ab370b124e:/# python3 -m pip install virtualenv
root@e9ab370b124e:/home/ssh_train# python3 -m virtualenv datagen
root@e9ab370b124e:/home/ssh_train# git clone https://github.com/erkansirin78/data-generator.git
root@e9ab370b124e:/home/ssh_train# source datagen/bin/activate
(datagen) root@e9ab370b124e:/home/ssh_train# cd data-generator
(datagen) root@e9ab370b124e:/home/ssh_train/data-generator# pip install -r requirements.txt
(datagen) root@e9ab370b124e:/home/ssh_train/data-generator# cd ..
(datagen) root@e9ab370b124e:/home/ssh_train# mkdir datasets
(datagen) root@e9ab370b124e:/home/ssh_train# cd datasets
(datagen) root@e9ab370b124e:/home/ssh_train/datasets# wget -O tmdb_5000_movies_and_credits.zip https://github.com/erkansirin78/datasets/raw/master/tmdb_5000_movies_and_credits.zip
(datagen) root@e9ab370b124e:/home/ssh_train/datasets# unzip tmdb_5000_movies_and_credits.zip
(datagen) root@e9ab370b124e:/home/ssh_train/datasets# rm -r tmdb_5000_movies_and_credits.zip
```
```
(datagen) root@e9ab370b124e:/# ls -l /opt/spark/jars
(datagen) root@e9ab370b124e:/# wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar &&\
mv delta-core_2.12-2.4.0.jar opt/spark/jars/ &&\
wget https://repo1.maven.org/maven2/io/delta/delta-storage/2.4.0/delta-storage-2.4.0.jar &&\
mv delta-storage-2.4.0.jar opt/spark/jars/
```


![](images/img_1.png)


![](images/img_2.png)
```
python dataframe_to_s3.py -buc tmdb-bronze \
-k credits/credits_part \
-aki root -sac root12345 \
-eu http://minio:9000 \
-i /home/ssh_train/datasets/tmdb_5000_movies_and_credits/tmdb_5000_credits.csv \
-ofp True -z 500 -b 0.1
```
```
python dataframe_to_s3.py -buc tmdb-bronze \
-k   movies/movies_part \
-aki root -sac root12345 \
-eu http://minio:9000 \
-i /home/ssh_train/datasets/tmdb_5000_movies_and_credits/tmdb_5000_movies.csv \
-ofp True -z 500 -b 0.1
```

### AIRFLOW
 ````
mkdir airflow_final
cd airflow_final

touch s3_spark_to_s3.py
docker cp s3_spark_to_s3.py spark_client:/

touch transformation_dag.py
docker cp transformation_dag.py airflow-scheduler:/opt/airflow/dags
````

