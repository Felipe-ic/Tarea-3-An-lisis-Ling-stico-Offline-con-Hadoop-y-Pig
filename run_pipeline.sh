

# 1. Extraer datos de la BDD/CSV a texto plano
echo "[1/4] Extrayendo datos..."
# Usamos el contenedor de persistor_bdd para correr el extractor ya que tiene las libs de Python
docker exec -it persistor_bdd python data_extractor.py

# 2. Preparar HDFS (Crear carpetas)
echo "[2/4] Preparando HDFS..."
docker exec -it pig_client hdfs dfs -mkdir -p /input
docker exec -it pig_client hdfs dfs -mkdir -p /output

# 3. Subir archivos al HDFS desde el contenedor pig_client
# (Como montamos el volumen .:/app en docker-compose, los archivos ya son visibles dentro del contenedor)
echo "[3/4] Subiendo archivos a HDFS..."
docker exec -it pig_client hdfs dfs -put -f /app/human_answers.txt /input/
docker exec -it pig_client hdfs dfs -put -f /app/llm_answers.txt /input/
docker exec -it pig_client hdfs dfs -put -f /app/stopwords.txt /input/

# 4. Instalar Pig y Ejecutar Análisis (Solo si no está instalado, la imagen base es raw)
echo "[4/4] Ejecutando Pig..."
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

echo "=== PROCESO FINALIZADO ==="
echo "Para ver los resultados ejecuta:"
echo "docker exec -it pig_client hdfs dfs -cat /output/human_top50/part-r-00000"
echo "docker exec -it pig_client hdfs dfs -cat /output/llm_top50/part-r-00000"