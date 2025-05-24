CREATE DATABASE IF NOT EXISTS cf; 
USE cf; 
CREATE TABLE submissions (
    id UInt64,
    handle String,
    verdict String,
    programmingLanguage String,
    passedTestCount UInt32,
    timeConsumedMillis UInt32,
    memoryConsumedBytes UInt64,
    contestId UInt32,
    problemIndex String,
    problemName String,
    problemPoints Float64,
    problemRating UInt32,
    problem_tag String
) ENGINE = MergeTree()
ORDER BY id;