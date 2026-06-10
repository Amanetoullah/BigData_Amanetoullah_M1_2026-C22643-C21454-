#!/bin/bash

echo "=========================================="
echo "Démarrage des conteneurs Docker..."
echo "=========================================="
docker-compose up -d

echo "Attente de l'initialisation de HDFS (30 secondes)..."
sleep 30

echo "=========================================="
echo "Préparation de HDFS..."
echo "=========================================="
docker exec namenode hdfs dfs -mkdir -p /user/hadoop/input

echo "=========================================="
echo "Copie du fichier CSV dans HDFS..."
echo "(Cette étape peut prendre quelques minutes pour 2,9 Go)"
echo "=========================================="
docker exec namenode hdfs dfs -put /data/yellow_tripdata_2015-03.csv /user/hadoop/input/yellow_tripdata_2015-03.csv

echo "=========================================="
echo "Lancement du script d'analyse PySpark..."
echo "=========================================="
docker exec spark-master /spark/bin/spark-submit /scripts/script_sujet1.py

echo "=========================================="
echo "Traitement terminé !"
echo "=========================================="
