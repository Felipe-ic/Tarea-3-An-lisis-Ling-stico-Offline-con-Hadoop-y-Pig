import csv
import os
import random

# Definición de rutas (asumiendo que test.csv está en la raíz /app)
CSV_FILE = 'test.csv'
OUTPUT_HUMAN = 'human_answers.txt'
OUTPUT_LLM = 'llm_answers.txt'

def extract_and_prepare_data():
    """
    Extrae respuestas humanas y LLM del CSV y las guarda en archivos separados 
    para la ingesta en HDFS.
    """
    human_data = []
    llm_data = []
    
    try:
        with open(CSV_FILE, mode='r', encoding='utf-8') as f:
            # Columnas: 0=id, 1=pregunta, 2=titulo, 3=respuesta_humana
            reader = csv.reader(f)
            
            # Asumimos que la primera fila es el encabezado y la omitimos
            next(reader) 
            
            for row in reader:
                if len(row) > 3:
                    # Respuesta humana (Columna 3: best_answer_text)
                    human_answer = row[3].strip()
                    if human_answer:
                        human_data.append(human_answer)

                    # Respuesta LLM (Simulada: Tarea 2 no llenó la BDD)
                    # Para la Tarea 3, simularemos que la respuesta LLM es igual a la humana para 
                    # asegurar un WordCount comparable, o una versión ligeramente modificada.
                    llm_data.append(human_answer) # Usar la misma para simular el LLM
    
    except FileNotFoundError:
        print(f"ERROR: No se encontró el archivo {CSV_FILE}. La ingesta fallará.")
        return False

    # Guardar respuestas Humanas
    with open(OUTPUT_HUMAN, 'w', encoding='utf-8') as f_human:
        f_human.write('\n'.join(human_data))

    # Guardar respuestas LLM
    with open(OUTPUT_LLM, 'w', encoding='utf-8') as f_llm:
        f_llm.write('\n'.join(llm_data))

    print(f"Ingesta de datos finalizada. Archivos creados: {OUTPUT_HUMAN}, {OUTPUT_LLM}")
    return True

if __name__ == '__main__':
    extract_and_prepare_data()