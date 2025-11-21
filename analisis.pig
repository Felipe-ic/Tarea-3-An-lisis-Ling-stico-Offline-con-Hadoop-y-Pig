

-- 1. Cargar la lista de stopwords
stopwords_raw = LOAD '/input/stopwords.txt' USING PigStorage() AS (word:chararray);

-- 2. Cargar las respuestas
human_raw = LOAD '/input/human_answers.txt' USING PigStorage('\n') AS (line:chararray);
llm_raw   = LOAD '/input/llm_answers.txt'   USING PigStorage('\n') AS (line:chararray);

-- MACRO CORREGIDA: Ahora recibe 'ref_stopwords' como segundo argumento
DEFINE ProcessText(raw_data, ref_stopwords) RETURNS processed {
    -- a. Tokenización
    words = FOREACH $raw_data GENERATE FLATTEN(TOKENIZE(LOWER(line))) AS word;
    
    -- b. Limpieza de puntuación
    clean_words = FOREACH words GENERATE REPLACE(word, '[^a-z]', '') AS word;
    non_empty = FILTER clean_words BY SIZE(word) > 1;

    -- c. Join usando el ARGUMENTO ($ref_stopwords), no la variable global
    joined = JOIN non_empty BY word LEFT, $ref_stopwords BY word USING 'replicated';

    -- d. Filtrar nulls
    filtered = FILTER joined BY $ref_stopwords::word IS NULL;

    -- e. Proyectar
    $processed = FOREACH filtered GENERATE non_empty::word AS word;
};

-- 3. Procesar (Pasamos 'stopwords_raw' como segundo parámetro)
human_words = ProcessText(human_raw, stopwords_raw);
llm_words   = ProcessText(llm_raw, stopwords_raw);

-- 4. Conteo de Frecuencia
-- Humanas
human_groups = GROUP human_words BY word;
human_counts = FOREACH human_groups GENERATE group AS word, COUNT(human_words) AS count;
human_ordered = ORDER human_counts BY count DESC;

-- LLM
llm_groups = GROUP llm_words BY word;
llm_counts = FOREACH llm_groups GENERATE group AS word, COUNT(llm_words) AS count;
llm_ordered = ORDER llm_counts BY count DESC;

-- 5. Almacenar (Top 50)
human_top = LIMIT human_ordered 50;
llm_top   = LIMIT llm_ordered 50;

STORE human_top INTO '/output/human_top50';
STORE llm_top INTO '/output/llm_top50';