# THE SYSTEM ARCHITECTURES

Created a pg_hba.conf to store networking details for our PostreSQL metadata database

Ensure you are working on the WSL file system and not windows file system, mnt/c/

build the containers:

cd ~/EndToEndDEproject
docker-compose build --no-cache

Once the build succeeds, start the containers:

docker-compose up -d

moving files n folders:
rsync -av --progress /mnt/c/Users/Administrator/Desktop/ALLMYTASKS/TSIDATASCIENCE/TeachingPython/BooksScrapping/EndToEndDEproject/ ~/EndToEndDEproject/

docker-compose 

Once you have configured the environment well, 
Initialize the database incase not initialized:
docker exec -it endtoenddeproject-webserver-1 airflow db init

