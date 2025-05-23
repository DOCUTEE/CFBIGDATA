CREATE DATABASE IF NOT EXISTS test; 

CREATE TABLE test.word_count ( 
  sentence String, 
  word_count UInt32, 
  event_time DateTime DEFAULT now() 
) ENGINE = MergeTree() 
ORDER BY event_time;