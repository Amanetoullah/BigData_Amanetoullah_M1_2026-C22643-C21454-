from pyspark.sql import SparkSession
import time
import subprocess

# --- 1. Initialisation de la session Spark ---
# Compatible Spark 3.1.1, connexion au master local (docker-compose)
spark = SparkSession.builder \
    .appName("Sujet1_Architecte_Stockage") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.parquet.filterPushdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("========== DEBUT DU TRAITEMENT : SUJET 1 ==========")

# Définition des chemins HDFS
# Le dataset doit se trouver dans ce dossier HDFS
input_csv = "hdfs://namenode:9000/user/hadoop/input/yellow_tripdata_2015-03.csv"
output_base = "hdfs://namenode:9000/user/hadoop/output"

# --- 1. INGESTION DU DATASET ---
print("\n--- 1. INGESTION DU FICHIER CSV ---")
print(f"Chargement depuis : {input_csv}")
# Inférer le schéma est crucial pour avoir des types corrects (ex: double pour fare_amount)
df_csv = spark.read.csv(input_csv, header=True, inferSchema=True)
df_csv.printSchema()

# Création d'une vue temporaire pour le CSV original
df_csv.createOrReplaceTempView("taxi_csv")


# --- 2. TRANSFORMATION ET ECRITURE PARQUET ---
print("\n--- 2. ECRITURE PARQUET AVEC DIFFERENTES COMPRESSIONS ---")
compressions = ["uncompressed", "snappy", "gzip"]

for comp in compressions:
    out_path = f"{output_base}/taxi_parquet_{comp}"
    print(f"\n[+] Ecriture en Parquet (Compression: {comp}) vers {out_path} ...")
    start_time = time.time()
    
    # Écriture HDFS
    df_csv.write.mode("overwrite") \
          .option("compression", comp) \
          .parquet(out_path)
          
    end_time = time.time()
    print(f"    -> Temps d'écriture ({comp}) : {end_time - start_time:.2f} secondes")


# --- 3. BENCHMARK DE VITESSE (CSV vs PARQUET SNAPPY) ---
print("\n--- 3. BENCHMARK DE VITESSE (REQUETE ANALYTIQUE) ---")
# Remarque importante : En PySpark, la méthode `spark.time()` n'existe pas nativement dans l'API 
# contrairement au shell Scala. Nous utilisons donc le module natif `time` de Python.

# Chargement du Parquet Snappy pour le benchmark
df_parquet_snappy = spark.read.parquet(f"{output_base}/taxi_parquet_snappy")
df_parquet_snappy.createOrReplaceTempView("taxi_parquet")

# Spark SQL est insensible à la casse par défaut. 
# La colonne sera trouvée qu'elle s'appelle 'VendorID' ou 'vendor_id'.
query = "SELECT AVG(fare_amount) FROM {} WHERE VendorID = 1"

print("\nExécution de la requête sur le CSV original (taxi_csv)...")
start_csv = time.time()
spark.sql(query.format("taxi_csv")).show()
end_csv = time.time()
time_csv = end_csv - start_csv
print(f"Temps d'exécution sur CSV : {time_csv:.4f} secondes")

print("\nExécution de la requête sur le format Parquet Snappy (taxi_parquet)...")
start_parquet = time.time()
spark.sql(query.format("taxi_parquet")).show()
end_parquet = time.time()
time_parquet = end_parquet - start_parquet
print(f"Temps d'exécution sur Parquet : {time_parquet:.4f} secondes")

if time_parquet > 0:
    boost = time_csv / time_parquet
    print(f"\n🚀 -> ACCELERATION (BOOST) : x{boost:.2f}")


# --- 4. ANALYSE DU PLAN D'EXECUTION (COLUMN PRUNING) ---
print("\n--- 4. ANALYSE DU PLAN D'EXECUTION (EXPLAIN) ---")
print("Dans la partie 'Physical Plan', observez le 'PushedFilters' (qui filtre le VendorID)")
print("et le 'ReadSchema' (qui ne lit que VendorID et fare_amount, montrant le Column Pruning).")
spark.sql(query.format("taxi_parquet")).explain(extended=True)


# --- 5. ANALYSE DU DATA SKEW ---
print("\n--- 5. ANALYSE DU DATA SKEW (REPARTITION DE CHARGE) ---")
def analyze_data_skew(hdfs_path):
    print(f"Inspection des tailles de fichiers dans : {hdfs_path}")
    try:
        sc = spark.sparkContext
        URI = sc._gateway.jvm.java.net.URI
        Path = sc._gateway.jvm.org.apache.hadoop.fs.Path
        FileSystem = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        
        fs = FileSystem.get(URI(hdfs_path), sc._jsc.hadoopConfiguration())
        file_status = fs.listStatus(Path(hdfs_path))
        
        sizes_mb = []
        for status in file_status:
            if status.isFile():
                file_name = status.getPath().getName()
                size_bytes = status.getLen()
                # On ignore le marqueur de succès vide de Spark (_SUCCESS)
                if size_bytes > 0 and "_SUCCESS" not in file_name:
                    size_mb = size_bytes / (1024 * 1024)
                    sizes_mb.append(size_mb)
                    print(f"  - {file_name} : {size_mb:.2f} MB")
        
        if sizes_mb:
            avg_size = sum(sizes_mb) / len(sizes_mb)
            max_size = max(sizes_mb)
            min_size = min(sizes_mb)
            print(f"\n-> Bilan Data Skew : Nb Fichiers = {len(sizes_mb)}")
            print(f"   Min = {min_size:.2f} MB | Max = {max_size:.2f} MB | Moyenne = {avg_size:.2f} MB")
            
            # Règle empirique de détection de Skew : le plus gros fichier dépasse 1.5x la moyenne
            if max_size > avg_size * 1.5 and len(sizes_mb) > 1:
                print("   ⚠️ AVERTISSEMENT : Potentiel Data Skew détecté (Déséquilibre de charge).")
            else:
                print("   ✅ Distribution des données équilibrée (Pas de Data Skew majeur).")
        else:
            print("   Aucun fichier de données trouvé.")
    except Exception as e:
        print(f"   Erreur d'exécution de l'analyse : {e}")

# Lancement de l'analyse sur le dossier de sortie Parquet Snappy
analyze_data_skew(f"{output_base}/taxi_parquet_snappy")

# Fermeture de la session
spark.stop()
print("\n========== FIN DU TRAITEMENT : SUJET 1 ==========")
