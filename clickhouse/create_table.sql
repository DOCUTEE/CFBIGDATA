CREATE DATABASE IF NOT EXISTS cf; 
USE cf; 
DROP TABLE IF EXISTS cf.submissions;
CREATE TABLE IF NOT EXISTS cf.submissions (
    id UInt64,
    relativeTimeSeconds UInt32,
    programmingLanguage String,
    verdict String,
    passedTestCount UInt32,
    timeConsumedMillis UInt32,
    memoryConsumedBytes UInt64,
    handle String,
    problem_contestId UInt32,
    problem_index String,
    problem_name String,
    problem_points UInt32,
    problem_rating UInt32,
    tag String
) ENGINE = MergeTree()
ORDER BY (id, tag)