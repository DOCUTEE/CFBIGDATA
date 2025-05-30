DROP DATABASE IF EXISTS cf;
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

DROP DATABASE IF EXISTS lakehouse;
CREATE DATABASE IF NOT EXISTS lakehouse; 
USE lakehouse;

CREATE TABLE dim_user (
    user_id String,
    user_handle String
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE dim_problem (
    problem_id String,
    contest_id String,
    problem_index String,
    problem_name String,
    points Float32,
    rating Float32
) ENGINE = MergeTree()
ORDER BY problem_id;

CREATE TABLE dim_verdict (
    verdict_id String,
    verdict_name String
) ENGINE = MergeTree()
ORDER BY verdict_id;


CREATE TABLE dim_language (
    language_id String,
    language_name String
) ENGINE = MergeTree()
ORDER BY language_id;

CREATE TABLE dim_tag (
    tag_id String,
    tag_name String
) ENGINE = MergeTree()
ORDER BY tag_id;

CREATE TABLE dim_tag_problem (
    problem_id String,
    tag_id String
) ENGINE = MergeTree()
ORDER BY (problem_id, tag_id);

CREATE TABLE fact_submission (
    submission_id Int32,
    user_id String,
    problem_id String,
    verdict_id String,
    language_id String,
    relative_time_seconds Int32,
    passed_test_count Int32,
    time_consumed_millis Float32,
    memory_consumed_bytes Float32
) ENGINE = MergeTree()
ORDER BY submission_id;
