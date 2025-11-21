# Tarea-3-An-lisis-Ling-stico-Offline-con-Hadoop-y-Pig

BAjar cualquier contenedor existente y luego levantarlos:

docker-compose down -v
docker-compose up -d


# 1. Extraer datos de la BDD/CSV a texto plano

# Usamos el contenedor de persistor_bdd para correr el extractor ya que tiene las libs de Python

docker exec -it persistor_bdd python data_extractor.py

# 2. Preparar HDFS (Crear carpetas)

docker exec -it pig_client hdfs dfs -mkdir -p /input
docker exec -it pig_client hdfs dfs -mkdir -p /output

# 3. Subir archivos al HDFS desde el contenedor pig_client
# (Como montamos el volumen .:/app en docker-compose, los archivos ya son visibles dentro del contenedor)

docker exec -it pig_client hdfs dfs -put -f /app/human_answers.txt /input/
docker exec -it pig_client hdfs dfs -put -f /app/llm_answers.txt /input/
docker exec -it pig_client hdfs dfs -put -f /app/stopwords.txt /input/

# 4. Instalar Pig y Ejecutar Análisis (Solo si no está instalado, la imagen base es raw)
# Nota: Instalamos Pig al vuelo porque la imagen hadoop-base es ligera. 
# En producción haríamos un Dockerfile custom, pero esto cumple para la tarea.
docker exec -it pig_client bash -c "
    if ! command -v pig &> /dev/null; then
        echo 'Instalando Pig...';
        apt-get update && apt-get install -y pig;
    fi
    
    echo 'Corriendo script...';
    pig -x mapreduce /app/analisis.pig
"


echo "Para ver los resultados ejecuta:"
echo "docker exec -it pig_client hdfs dfs -cat /output/human_top50/part-r-00000"
echo "docker exec -it pig_client hdfs dfs -cat /output/llm_top50/part-r-00000"
