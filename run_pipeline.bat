@echo off
echo ==========================================
echo Demarrage des conteneurs Docker...
echo ==========================================
docker-compose up -d

echo Attente de l'initialisation de HDFS (30 secondes)...
ping 127.0.0.1 -n 31 > nul

echo ==========================================
echo Preparation de HDFS...
echo ==========================================
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/input

echo ==========================================
echo Copie du fichier CSV dans HDFS...
echo (Cette etape peut prendre quelques minutes pour 2 Go)
echo ==========================================
docker exec namenode hdfs dfs -put /data/yellow_tripdata_2015-03.csv /user/hadoop/input/yellow_tripdata_2015-03.csv

echo ==========================================
echo Lancement du script d'analyse PySpark...
echo ==========================================
docker exec spark-master /spark/bin/spark-submit /scripts/script_sujet1.py

echo ==========================================
echo Traitement termine !
echo ==========================================
pause
