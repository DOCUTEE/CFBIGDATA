CREATE DATABASE IF NOT EXISTS cf; 
USE cf; 
DROP TABLE IF EXISTS cf.submissions;
CREATE TABLE cf.submissions
(
    id UInt64,
    relativeTime DateTime,
    programmingLanguage String,
    verdict String,
    passedTestCount Int32,
    timeConsumedMillis Int32,
    memoryConsumedBytes UInt64,
    handle String,
    problem_contestId Int32,
    problem_index String,
    problem_name String,
    problem_points Int32,
    problem_rating Int32,
    tag String
)
ENGINE = MergeTree
ORDER BY id;
