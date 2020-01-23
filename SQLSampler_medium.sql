-- file: SQLSampler_medium.sql
-- purpse: sample authors, submissions, comments, subreddits from reddit database
-- author: Alexander Ruch
-- update: 4/15/2019

-- NOTE: The Result numbers reported below accurately reflect the 20el sampling
-- NOTE: Get an approximate row count with...
--         SELECT reltuples AS approximate_row_count FROM pg_class WHERE relname = 'table_name';

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- SUICIDEWATCH SAMPLER
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- BURN 1
-- Select SW authors/submissions (excluding missing cases)
--   Note: Only select if author has >= 20 submissions total
--   Note: TABLESAMPLE SYSTEM was not significantly faster
CREATE TEMP TABLE temp_main_pre (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_pre SET(parallel_workers = 32);
WITH auths AS (
    SELECT submissions.author
    FROM submissions
    GROUP BY submissions.author HAVING COUNT(*) > 19
)
    INSERT INTO temp_main_pre (author, id, title, selftext, created_utc, subreddit)
        SELECT DISTINCT ON (s.author) s.author, s.id, s.title, s.selftext,
        s.created_utc, s.subreddit
            FROM submissions AS s, auths
            WHERE s.subreddit = 'SuicideWatch'
            AND s.author IS NOT NULL
            AND s.author NOT LIKE '[deleted]'
            AND s.author IN (auths.author);
-- Result: 24,281
SELECT * FROM temp_main_pre LIMIT 100;
-- Sample a % of SW subm authors from temp_main_pre
CREATE TEMP TABLE temp_main AS
    SELECT * FROM temp_main_pre
        TABLESAMPLE BERNOULLI (20)
        REPEATABLE (407);
-- Result: 4,948
ALTER TABLE temp_main SET(parallel_workers = 32);
-- Check temp_main
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_pre;
-- Create a copy of sw authors for later tables
CREATE TEMP TABLE temp_auths AS
    SELECT authors FROM temp_main;
-- Result: 4,948

-- BURN 2
-- Select other SUBMISSIONS from SW authors (excluding burn 1 rows)
CREATE TEMP TABLE temp_main_swauth (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_swauth SET(parallel_workers = 32);
INSERT INTO temp_main_swauth (author, id, title, selftext, created_utc, subreddit)
    SELECT s.author, s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM temp_main AS ts
        INNER JOIN submissions AS s
        ON ts.author = s.author
        WHERE ts.id != s.id;
-- Result: 777,243 (= 94 subm/auth)
-- Sample  a % of SW authors' other submissions
INSERT INTO temp_main (author, id, title, selftext, created_utc, subreddit)
    SELECT * FROM temp_main_swauth
        TABLESAMPLE BERNOULLI (20)
        REPEATABLE (407);
-- Result: 155,646 (= 31 subm/auth)
-- Check table
SELECT COUNT(*) FROM temp_main;
-- Result: 160,594 (=155,646 + 4,948; = 32 subm/auth)
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_swauth;
CREATE INDEX idx_tm_sauth ON temp_main (author);
CREATE INDEX idx_tm_sid ON temp_main (id);

-- Get submission info for all submissions on which SW authors commented
CREATE TEMP TABLE temp_coms_subms_SW (
    submission_id VARCHAR(20),
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_body TEXT,
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_coms_subms_SW SET(parallel_workers = 32);
WITH subms AS (SELECT DISTINCT ON (temp_main.author) author FROM temp_main)
    INSERT INTO temp_coms_subms_SW (
            submission_id,
            comment_author,
            comment_id,
            comment_body,
            comment_created_utc,
            subreddit
        )
        SELECT c.link_id, c.author, c.id, c.body, c.created_utc, c.subreddit
            FROM comments AS c, subms
            WHERE c.author IN (subms.author);
-- Result: 9,611,359 (= 1942 coms-subms/auth)
SELECT * FROM temp_coms_subms_SW LIMIT 100;
-- Remove multiple comments to same submissions, sample a % of comments
CREATE TEMP TABLE temp_coms_subms_SWd AS
    SELECT DISTINCT ON (temp_coms_subms_SW.submission_id) submission_id
        FROM temp_coms_subms_SW
        TABLESAMPLE BERNOULLI (20)
        REPEATABLE (407);
-- Result: 1,415,357 (= 286 distinct coms-subms/auth)
ALTER TABLE temp_coms_subms_SWd SET(parallel_workers = 32);
SELECT * FROM temp_coms_subms_SWd LIMIT 100;
DROP TABLE temp_coms_subms_SW;

-- add subm ids from SW auths' subms to subm ids of subms SW auths commented on
-- Note: removes duplicates in temp_subms_full
CREATE TEMP TABLE temp_subms AS
    SELECT temp_main.id FROM temp_main
    UNION
    SELECT SUBSTRING(temp_coms_subms_SWd.submission_id,4) FROM temp_coms_subms_SWd;
-- Result: 1,547,570 (= 160,594 + 1,415,357 - dups/self-comments)
ALTER TABLE temp_subms SET(parallel_workers = 32);
SELECT * FROM temp_subms LIMIT 100;
DROP TABLE temp_coms_subms_SWd;
DROP TABLE temp_main;
CREATE TEMP TABLE temp_subms_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_title TEXT,
    submission_selftext TEXT,
    submission_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_subms_full SET(parallel_workers = 32);
WITH ts AS (SELECT DISTINCT ON (temp_subms.id) id FROM temp_subms)
    INSERT INTO temp_subms_full (
            submission_author, submission_id, submission_title, submission_selftext,
            submission_created_utc, subreddit
        )
        SELECT s.author, 't3_'||s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM submissions AS s, ts
        WHERE s.id IN (ts.id)
        AND s.author IS NOT NULL
        AND s.author NOT LIKE '[deleted]';
-- Result: 1,369,633 (some authors were deleted)
SELECT * FROM temp_subms_full LIMIT 100;
DROP TABLE temp_subms;

-- Join submissions and all comments
CREATE TEMP TABLE temp_main_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_full SET(parallel_workers = 32);
INSERT INTO temp_main_full (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT s.submission_author, SUBSTRING(s.submission_id,4),
        s.submission_created_utc, c.author, c.id, c.created_utc, s.subreddit
        FROM comments AS c
        INNER JOIN temp_subms_full AS s
        ON c.link_id = s.submission_id
        WHERE c.author IS NOT NULL
        AND c.author NOT LIKE '[deleted]';
-- Result: 447,579,856
SELECT * FROM temp_main_full LIMIT 100;
CREATE INDEX idx_tmf_cid ON temp_main_full (comment_id);
CREATE INDEX idx_tmf_sid ON temp_main_full (submission_id);
DROP TABLE temp_subms_full;
SELECT pg_size_pretty(pg_relation_size('temp_main_full'));
-- Result: 42GB (sample_main = 1GB, submissions = 193GB, comments = 1177GB)

-- Add sw authors' submissions and comments to table
CREATE TABLE sample_main_20el (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE sample_main_20el SET(parallel_workers = 32);
-- Make sure all SW authors' submissions have at least the first comment
WITH swauths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_main_20el (
            submission_id,
            submission_author,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id) sm.submission_id,
            sm.submission_author, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, swauths
            WHERE sm.submission_author IN (swauths.author)
            ORDER BY sm.submission_id, sm.comment_created_utc ASC;
-- Result: 229,928
SELECT * FROM sample_main_20el LIMIT 100;
-- Make sure SW authors' first comments on submissions are in the table
WITH swauths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_main_20el (
            submission_author,
            submission_id,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id, sm.comment_author) sm.submission_author,
            sm.submission_id, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, swauths
            WHERE sm.comment_author IN (swauths.author)
            ORDER BY sm.submission_id, sm.comment_author, sm.comment_created_utc ASC;
-- Result: 1,879,465 (vs 1,369,633 from temp_subms_full)
SELECT * FROM sample_main_20el OFFSET 229900 LIMIT 100;
SELECT COUNT(*) FROM sample_main_20el;
-- Result: 2,109,393 (= 229,928 + 1,879,465)
-- Sample 1% of other comments on all submissions
INSERT INTO sample_main_20el (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT sm.submission_author, sm.submission_id, sm.submission_created_utc,
        sm.comment_author, sm.comment_id, sm.comment_created_utc, sm.subreddit
        FROM temp_main_full AS sm
        TABLESAMPLE BERNOULLI (1)
        REPEATABLE (407);
-- Result: 4,475,200
SELECT * FROM sample_main_20el OFFSET 2109350 LIMIT 100;
SELECT COUNT(*) FROM sample_main_20el;
-- Result: 6,584,593 (= 2,109,393 + 4,475,200)
SELECT pg_size_pretty(pg_relation_size('sample_main_20el'));
-- Result: 577 MB






--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- MENTAL HEALTH SAMPLER
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- BURN 1
-- Select mh authors/submissions (excluding missing cases)
--   Note: Only select if author has >= 20 submissions total
CREATE TEMP TABLE temp_main_pre (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_pre SET(parallel_workers = 32);
WITH auths AS (
    SELECT submissions.author
    FROM submissions
    WHERE submissions.author IS NOT NULL
    AND submissions.author NOT LIKE '[deleted]'
    GROUP BY submissions.author HAVING COUNT(*) > 19
)
    INSERT INTO temp_main_pre (author, id, title, selftext, created_utc, subreddit)
        SELECT DISTINCT ON (s.subreddit, s.author) s.author, s.id, s.title, s.selftext,
        s.created_utc, s.subreddit
            FROM submissions AS s, auths
            WHERE (s.author IN (auths.author))
            AND (s.subreddit = 'mentalhealth'
            OR s.subreddit = 'mentalillness'
            OR s.subreddit = 'addiction'
            OR s.subreddit = 'alcoholism'
            OR s.subreddit = 'Anger'
            OR s.subreddit = 'Anxiety'
            OR s.subreddit = 'BipolarReddit'
            OR s.subreddit = 'depression'
            OR s.subreddit = 'domesticviolence'
            OR s.subreddit = 'EatingDisorders'
            OR s.subreddit = 'OCD'
            OR s.subreddit = 'Phobia'
            OR s.subreddit = 'psychoticreddit'
            OR s.subreddit = 'ptsd'
            OR s.subreddit = 'schizophrenia'
            OR s.subreddit = 'survivorsofabuse'
            OR s.subreddit = 'rape'
            OR s.subreddit = 'OpiatesRecovery'
            OR s.subreddit = 'ADHD'
            OR s.subreddit = 'itgetsbetter');
-- Result: 161,863 (vs 24,281 in sw)
SELECT * FROM temp_main_pre LIMIT 100;
-- Sample a % of mh subm authors from temp_main_pre
CREATE TEMP TABLE temp_main AS
    SELECT * FROM temp_main_pre
        TABLESAMPLE BERNOULLI (5)
        REPEATABLE (407);
-- Result: 8,194 (vs 4,948 in sw)
ALTER TABLE temp_main SET(parallel_workers = 32);
-- Check temp_main
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_pre;
-- Create a copy of mh authors for later tables
CREATE TEMP TABLE temp_auths AS
    SELECT DISTINCT ON (temp_main.author) temp_main.author FROM temp_main;
-- Result: 8,101 (some authors posted in more than 1 mh subreddit)

-- BURN 2
-- Select other SUBMISSIONS from mh authors (excluding burn 1 rows)
CREATE TEMP TABLE temp_main_mhauth (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_mhauth SET(parallel_workers = 32);
INSERT INTO temp_main_mhauth (author, id, title, selftext, created_utc, subreddit)
    SELECT s.author, s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM temp_main AS ts
        INNER JOIN submissions AS s
        ON ts.author = s.author
        WHERE ts.id != s.id;
-- Result: 1,276,677 (= 158 subm/auth) [vs 777,243 & 94 subm/auth in sw]
-- Sample a % of mh authors' other submissions
INSERT INTO temp_main (author, id, title, selftext, created_utc, subreddit)
    SELECT * FROM temp_main_mhauth
        TABLESAMPLE BERNOULLI (15)
        REPEATABLE (407);
-- Result: 191,439 (= 27 subm/auth) [vs 155,646 & 31 subm/auth in sw]
-- Check table
SELECT COUNT(*) FROM temp_main;
-- Result: 199,633 (= 191,439 + 8,194; = 25 subm/auth) [vs 160,594 & 32 subm/auth in sw]
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_mhauth;

-- Get submission info for all submissions on which mh authors commented
CREATE TEMP TABLE temp_coms_subms_mh (
    submission_id VARCHAR(20),
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_body TEXT,
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_coms_subms_mh SET(parallel_workers = 32);
WITH subms AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO temp_coms_subms_mh (
            submission_id,
            comment_author,
            comment_id,
            comment_body,
            comment_created_utc,
            subreddit
        )
        SELECT c.link_id, c.author, c.id, c.body, c.created_utc, c.subreddit
            FROM comments AS c, subms
            WHERE c.author IN (subms.author);
-- Result: 12,458,863 (= 1538 coms-subms/auth) [vs 9,611,359 & 1942 coms-subms/auth]
SELECT * FROM temp_coms_subms_mh LIMIT 100;
-- Remove multiple comments to same submissions, sample a % of comments
CREATE TEMP TABLE temp_coms_subms_mhd AS
    SELECT DISTINCT ON (temp_coms_subms_mh.submission_id) submission_id
        FROM temp_coms_subms_mh
        TABLESAMPLE BERNOULLI (15)
        REPEATABLE (407);
-- Result: 1,453,299 (= 179 distinct coms-subms/auth) [1,415,357 & 286]
ALTER TABLE temp_coms_subms_mhd SET(parallel_workers = 32);
SELECT * FROM temp_coms_subms_mhd LIMIT 100;
DROP TABLE temp_coms_subms_mh;

-- add subm ids from mh auths' subms to subm ids of subms mh auths commented on
-- Note: removes duplicates in temp_subms_full
CREATE TEMP TABLE temp_subms AS
    SELECT temp_main.id FROM temp_main
    UNION
    SELECT SUBSTRING(temp_coms_subms_mhd.submission_id,4) FROM temp_coms_subms_mhd;
-- Result: 1,627,084 (= 199,633 + 1,453,299 - dups/self-comments) [vs 1,547,570]
ALTER TABLE temp_subms SET(parallel_workers = 32);
SELECT * FROM temp_subms LIMIT 100;
DROP TABLE temp_coms_subms_mhd;
DROP TABLE temp_main;
CREATE TEMP TABLE temp_subms_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_title TEXT,
    submission_selftext TEXT,
    submission_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_subms_full SET(parallel_workers = 32);
WITH ts AS (SELECT DISTINCT ON (temp_subms.id) id FROM temp_subms)
    INSERT INTO temp_subms_full (
            submission_author, submission_id, submission_title, submission_selftext,
            submission_created_utc, subreddit
        )
        SELECT s.author, 't3_'||s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM submissions AS s, ts
        WHERE s.id IN (ts.id)
        AND s.author IS NOT NULL
        AND s.author NOT LIKE '[deleted]';
-- Result: 1,459,403 (some authors were deleted) [vs 1,369,633]
SELECT * FROM temp_subms_full LIMIT 100;
DROP TABLE temp_subms;

-- Join submissions and all comments
CREATE TEMP TABLE temp_main_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_full SET(parallel_workers = 32);
INSERT INTO temp_main_full (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT s.submission_author, SUBSTRING(s.submission_id,4),
        s.submission_created_utc, c.author, c.id, c.created_utc, s.subreddit
        FROM comments AS c
        INNER JOIN temp_subms_full AS s
        ON c.link_id = s.submission_id
        WHERE c.author IS NOT NULL
        AND c.author NOT LIKE '[deleted]';
-- Result: 458,653,466 [vs 447,579,856]
SELECT * FROM temp_main_full LIMIT 100;
CREATE INDEX idx_tmf_cid ON temp_main_full (comment_id);
CREATE INDEX idx_tmf_sid ON temp_main_full (submission_id);
DROP TABLE temp_subms_full;
SELECT pg_size_pretty(pg_relation_size('temp_main_full'));
-- Result: 39GB [vs 42GB]

-- Add mh authors' submissions and comments to table
CREATE TABLE sample_mentalhealth_20el (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE sample_mentalhealth_20el SET(parallel_workers = 32);
-- Make sure all mh authors' submissions have at least the first comment
WITH auths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_mentalhealth_20el (
            submission_id,
            submission_author,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id) sm.submission_id,
            sm.submission_author, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, auths
            WHERE sm.submission_author IN (auths.author)
            ORDER BY sm.submission_id, sm.comment_created_utc ASC;
-- Result: 273,514 [vs 229,928]
SELECT * FROM sample_mentalhealth_20el LIMIT 100;
-- Make sure mh authors' first comments on submissions are in the table
WITH auths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_mentalhealth_20el (
            submission_author,
            submission_id,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id, sm.comment_author) sm.submission_author,
            sm.submission_id, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, auths
            WHERE sm.comment_author IN (auths.author)
            ORDER BY sm.submission_id, sm.comment_author, sm.comment_created_utc ASC;
-- Result: 2,175,531 (vs 1,459,403 from temp_subms_full) [vs 1,879,465]
SELECT * FROM sample_mentalhealth_20el OFFSET 273500 LIMIT 100;
SELECT COUNT(*) FROM sample_mentalhealth_20el;
-- Result: 2,449,045 (= 273,514 + 2,175,531) [vs 2,109,393]
-- Sample 1% of other comments on all submissions
INSERT INTO sample_mentalhealth_20el (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT sm.submission_author, sm.submission_id, sm.submission_created_utc,
        sm.comment_author, sm.comment_id, sm.comment_created_utc, sm.subreddit
        FROM temp_main_full AS sm
        TABLESAMPLE BERNOULLI (1)
        REPEATABLE (407);
-- Result: 4,586,859 [vs 4,475,200]
SELECT * FROM sample_mentalhealth_20el OFFSET 2449000 LIMIT 100;
SELECT COUNT(*) FROM sample_mentalhealth_20el;
-- Result: 7,035,904 (= 2,449,045 + 4,586,859) [vs 6,584,593]
SELECT pg_size_pretty(pg_relation_size('sample_mentalhealth_20el'));
-- Result: 618MB [vs 577MB]






--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- SELFHELP SAMPLER
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- BURN 1
-- Select sh authors/submissions (excluding missing cases)
--   Note: Only select if author has >= 20 submissions total
CREATE TEMP TABLE temp_main_pre (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_pre SET(parallel_workers = 32);
WITH auths AS (
    SELECT submissions.author
    FROM submissions
    WHERE submissions.author IS NOT NULL
    AND submissions.author NOT LIKE '[deleted]'
    GROUP BY submissions.author HAVING COUNT(*) > 19
)
    INSERT INTO temp_main_pre (author, id, title, selftext, created_utc, subreddit)
        SELECT DISTINCT ON (s.subreddit, s.author) s.author, s.id, s.title, s.selftext,
        s.created_utc, s.subreddit
            FROM submissions AS s, auths
            WHERE (s.author IN (auths.author))
            AND (s.subreddit = 'selfimprovement'
            OR subreddit = 'zenhabits'
            OR subreddit = 'personalfinance'
            OR subreddit = 'productivity'
            OR subreddit = 'frugal'
            OR subreddit = 'decidingtobebetter'
            OR subreddit = 'GetMotivated'
            OR subreddit = 'getdisciplined'
            OR subreddit = 'LifeProTips'
            OR subreddit = 'LifeImprovement');
-- Result: 318,524 (vs 24,281 in sw)
SELECT * FROM temp_main_pre LIMIT 100;
-- Sample a % of sh subm authors from temp_main_pre
CREATE TEMP TABLE temp_main AS
    SELECT * FROM temp_main_pre
        TABLESAMPLE BERNOULLI (2.5)
        REPEATABLE (407);
-- Result: 8002 (vs 4,948 in sw)
ALTER TABLE temp_main SET(parallel_workers = 32);
-- Check temp_main
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_pre;
-- Create a copy of sh authors for later tables
CREATE TEMP TABLE temp_auths AS
    SELECT DISTINCT ON (temp_main.author) temp_main.author FROM temp_main;
-- Result: 7972 (some authors posted in more than 1 sh subreddit)

-- BURN 2
-- Select other SUBMISSIONS from sh authors (excluding burn 1 rows)
CREATE TEMP TABLE temp_main_shauth (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_shauth SET(parallel_workers = 32);
INSERT INTO temp_main_shauth (author, id, title, selftext, created_utc, subreddit)
    SELECT s.author, s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM temp_main AS ts
        INNER JOIN submissions AS s
        ON ts.author = s.author
        WHERE ts.id != s.id;
-- Result: 1,281,625 (= 161 subm/auth) [vs 777,243 & 94 subm/auth in sw]
-- Sample a % of sh authors' other submissions
INSERT INTO temp_main (author, id, title, selftext, created_utc, subreddit)
    SELECT * FROM temp_main_shauth
        TABLESAMPLE BERNOULLI (15)
        REPEATABLE (407);
-- Result: 192,182 (= 24 subm/auth) [vs 155,646 & 31 subm/auth in sw]
-- Check table
SELECT COUNT(*) FROM temp_main;
-- Result: 200,184 (= 192,182 + 8,001; = 25 subm/auth) [vs 160,594 & 32 subm/auth in sw]
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_shauth;

-- Get submission info for all submissions on which sh authors commented
CREATE TEMP TABLE temp_coms_subms_sh (
    submission_id VARCHAR(20),
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_body TEXT,
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_coms_subms_sh SET(parallel_workers = 32);
WITH subms AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO temp_coms_subms_sh (
            submission_id,
            comment_author,
            comment_id,
            comment_body,
            comment_created_utc,
            subreddit
        )
        SELECT c.link_id, c.author, c.id, c.body, c.created_utc, c.subreddit
            FROM comments AS c, subms
            WHERE c.author IN (subms.author);
-- Result: 12,451,679 (= 1562 coms-subms/auth) [vs 9,611,359 & 1942 coms-subms/auth]
SELECT * FROM temp_coms_subms_sh LIMIT 100;
-- Remove multiple comments to same submissions, sample a % of comments
CREATE TEMP TABLE temp_coms_subms_shd AS
    SELECT DISTINCT ON (temp_coms_subms_sh.submission_id) submission_id
        FROM temp_coms_subms_sh
        TABLESAMPLE BERNOULLI (15)
        REPEATABLE (407);
-- Result: 1,441,569 (= 181 distinct coms-subms/auth) [1,415,357 & 286]
ALTER TABLE temp_coms_subms_shd SET(parallel_workers = 32);
SELECT * FROM temp_coms_subms_shd LIMIT 100;
DROP TABLE temp_coms_subms_sh;

-- add subm ids from sh auths' subms to subm ids of subms sh auths commented on
-- Note: removes duplicates in temp_subms_full
CREATE TEMP TABLE temp_subms AS
    SELECT temp_main.id FROM temp_main
    UNION
    SELECT SUBSTRING(temp_coms_subms_shd.submission_id,4) FROM temp_coms_subms_shd;
-- Result: 1,618,746 (= 200,184 + 1,441,569 - dups/self-comments) [vs 1,547,570]
ALTER TABLE temp_subms SET(parallel_workers = 32);
SELECT * FROM temp_subms LIMIT 100;
DROP TABLE temp_coms_subms_shd;
DROP TABLE temp_main;
CREATE TEMP TABLE temp_subms_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_title TEXT,
    submission_selftext TEXT,
    submission_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_subms_full SET(parallel_workers = 32);
WITH ts AS (SELECT DISTINCT ON (temp_subms.id) id FROM temp_subms)
    INSERT INTO temp_subms_full (
            submission_author, submission_id, submission_title, submission_selftext,
            submission_created_utc, subreddit
        )
        SELECT s.author, 't3_'||s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM submissions AS s, ts
        WHERE s.id IN (ts.id)
        AND s.author IS NOT NULL
        AND s.author NOT LIKE '[deleted]';
-- Result: 1,465,391 (some authors were deleted) [vs 1,369,633]
SELECT * FROM temp_subms_full LIMIT 100;
DROP TABLE temp_subms;

-- Join submissions and all comments
CREATE TEMP TABLE temp_main_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_full SET(parallel_workers = 32);
INSERT INTO temp_main_full (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT s.submission_author, SUBSTRING(s.submission_id,4),
        s.submission_created_utc, c.author, c.id, c.created_utc, s.subreddit
        FROM comments AS c
        INNER JOIN temp_subms_full AS s
        ON c.link_id = s.submission_id
        WHERE c.author IS NOT NULL
        AND c.author NOT LIKE '[deleted]';
-- Result: 495,569,540 [vs 447,579,856]
SELECT * FROM temp_main_full LIMIT 100;
CREATE INDEX idx_tmf_cid ON temp_main_full (comment_id);
CREATE INDEX idx_tmf_sid ON temp_main_full (submission_id);
DROP TABLE temp_subms_full;
SELECT pg_size_pretty(pg_relation_size('temp_main_full'));
-- Result: 42GB [vs 42GB]

-- Add sh authors' submissions and comments to table
CREATE TABLE sample_selfhelp_20el (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE sample_selfhelp_20el SET(parallel_workers = 32);
-- Make sure all sh authors' submissions have at least the first comment
WITH auths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_selfhelp_20el (
            submission_id,
            submission_author,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id) sm.submission_id,
            sm.submission_author, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, auths
            WHERE sm.submission_author IN (auths.author)
            ORDER BY sm.submission_id, sm.comment_created_utc ASC;
-- Result: 260,598 [vs 229,928]
SELECT * FROM sample_selfhelp_20el LIMIT 100;
-- Make sure sh authors' first comments on submissions are in the table
WITH auths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_selfhelp_20el (
            submission_author,
            submission_id,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id, sm.comment_author) sm.submission_author,
            sm.submission_id, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, auths
            WHERE sm.comment_author IN (auths.author)
            ORDER BY sm.submission_id, sm.comment_author, sm.comment_created_utc ASC;
-- Result: 2,361,065 (vs 1,465,391 from temp_subms_full) [vs 1,879,465]
SELECT * FROM sample_selfhelp_20el OFFSET 260550 LIMIT 100;
SELECT COUNT(*) FROM sample_selfhelp_20el;
-- Result: 2,621,663 (= 260,598 + 2,361,065) [vs 2,109,393]
-- Sample 1% of other comments on all submissions
INSERT INTO sample_selfhelp_20el (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT sm.submission_author, sm.submission_id, sm.submission_created_utc,
        sm.comment_author, sm.comment_id, sm.comment_created_utc, sm.subreddit
        FROM temp_main_full AS sm
        TABLESAMPLE BERNOULLI (1)
        REPEATABLE (407);
-- Result: 4,954,568 [vs 4,475,200]
SELECT * FROM sample_selfhelp_20el OFFSET 2621650 LIMIT 100;
SELECT COUNT(*) FROM sample_selfhelp_20el;
-- Result: 7,576,231 (= 2,621,663 + 4,954,568) [vs 6,584,593]
SELECT pg_size_pretty(pg_relation_size('sample_selfhelp_20el'));
-- Result: 664MB [vs 577MB]






--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- RANDOM SAMPLER
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- Import system row sampler (will raise error if it already exists)
CREATE EXTENSION tsm_system_rows;

-- BURN 1
-- Select random authors/submissions (excluding missing cases)
--   Note: Only select if author has >= 20 submissions total
CREATE TEMP TABLE temp_main_pre (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_pre SET(parallel_workers = 32);
WITH auths AS (
    SELECT submissions.author
    FROM submissions
    WHERE submissions.author IS NOT NULL
    AND submissions.author NOT LIKE '[deleted]'
    GROUP BY submissions.author HAVING COUNT(*) > 19
)
    INSERT INTO temp_main_pre (author, id, title, selftext, created_utc, subreddit)
        SELECT DISTINCT ON (s.subreddit, s.author) s.author, s.id, s.title, s.selftext,
        s.created_utc, s.subreddit
            FROM submissions AS s, auths
            WHERE s.author IN (auths.author);
-- Result: 62,114,316 (vs 24,281 in sw)
SELECT * FROM temp_main_pre LIMIT 100;
-- Sample a % of random subm authors from temp_main_pre
CREATE TEMP TABLE temp_main AS
    SELECT * FROM temp_main_pre
        TABLESAMPLE SYSTEM_ROWS (5000);
-- Result: 5000 (vs 4,948 in sw)
ALTER TABLE temp_main SET(parallel_workers = 32);
-- Check temp_main
SELECT * FROM temp_main LIMIT 100;
-- Create a copy of random authors for later tables
CREATE TEMP TABLE temp_auths AS
    SELECT DISTINCT ON (temp_main.author) temp_main.author FROM temp_main;
-- Result: 4981 (some authors posted in more than 1 subreddit)

-- BURN 2
-- Select other SUBMISSIONS from random authors (excluding burn 1 rows)
CREATE TEMP TABLE temp_main_rauth (
    author VARCHAR(30),
    id VARCHAR(20),
    title TEXT,
    selftext TEXT,
    created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_rauth SET(parallel_workers = 32);
INSERT INTO temp_main_rauth (author, id, title, selftext, created_utc, subreddit)
    SELECT s.author, s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM temp_main AS ts
        INNER JOIN submissions AS s
        ON ts.author = s.author
        WHERE ts.id != s.id;
-- Result: 1,246,346 (= 250 subm/auth) [vs 777,243 & 94 subm/auth in sw]
-- Sample a % of random authors' other submissions
INSERT INTO temp_main (author, id, title, selftext, created_utc, subreddit)
    SELECT * FROM temp_main_rauth
        TABLESAMPLE BERNOULLI (15)
        REPEATABLE (407);
-- Result: 187,215 (= 38 subm/auth) [vs 155,646 & 31 subm/auth in sw]
-- Check table
SELECT COUNT(*) FROM temp_main;
-- Result: 192,215 (= 187,215 + 5,000; = 39 subm/auth) [vs 160,594 & 32 subm/auth in sw]
SELECT * FROM temp_main LIMIT 100;
DROP TABLE temp_main_pre;
DROP TABLE temp_main_rauth;

-- Get submission info for all submissions on which random authors commented
CREATE TEMP TABLE temp_coms_subms_r (
    submission_id VARCHAR(20),
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_body TEXT,
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_coms_subms_r SET(parallel_workers = 32);
WITH subms AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO temp_coms_subms_r (
            submission_id,
            comment_author,
            comment_id,
            comment_body,
            comment_created_utc,
            subreddit
        )
        SELECT c.link_id, c.author, c.id, c.body, c.created_utc, c.subreddit
            FROM comments AS c, subms
            WHERE c.author IN (subms.author);
-- Result: 9,432,833 (= 1893 coms-subms/auth) [vs 9,611,359 & 1942 coms-subms/auth]
SELECT * FROM temp_coms_subms_r LIMIT 100;
-- Remove multiple comments to same submissions, sample a % of comments
CREATE TEMP TABLE temp_coms_subms_rd AS
    SELECT DISTINCT ON (temp_coms_subms_r.submission_id) submission_id
        FROM temp_coms_subms_r
        TABLESAMPLE BERNOULLI (15)
        REPEATABLE (407);
-- Result: 1,130,124 (= 227 distinct coms-subms/auth) [1,415,357 & 286]
ALTER TABLE temp_coms_subms_rd SET(parallel_workers = 32);
SELECT * FROM temp_coms_subms_rd LIMIT 100;
DROP TABLE temp_coms_subms_r;

-- add subm ids from sh auths' subms to subm ids of subms sh auths commented on
-- Note: removes duplicates in temp_subms_full
CREATE TEMP TABLE temp_subms AS
    SELECT temp_main.id FROM temp_main
    UNION
    SELECT SUBSTRING(temp_coms_subms_rd.submission_id,4) FROM temp_coms_subms_rd;
-- Result: 1,301,464 (= 192,215 + 1,130,124 - dups/self-comments) [vs 1,547,570]
ALTER TABLE temp_subms SET(parallel_workers = 32);
SELECT * FROM temp_subms LIMIT 100;
DROP TABLE temp_coms_subms_rd;
DROP TABLE temp_main;
CREATE TEMP TABLE temp_subms_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_title TEXT,
    submission_selftext TEXT,
    submission_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_subms_full SET(parallel_workers = 32);
WITH ts AS (SELECT DISTINCT ON (temp_subms.id) id FROM temp_subms)
    INSERT INTO temp_subms_full (
            submission_author, submission_id, submission_title, submission_selftext,
            submission_created_utc, subreddit
        )
        SELECT s.author, 't3_'||s.id, s.title, s.selftext, s.created_utc, s.subreddit
        FROM submissions AS s, ts
        WHERE s.id IN (ts.id)
        AND s.author IS NOT NULL
        AND s.author NOT LIKE '[deleted]';
-- Result: 1,182,614 (some authors were deleted) [vs 1,369,633]
SELECT * FROM temp_subms_full LIMIT 100;
DROP TABLE temp_subms;

-- Join submissions and all comments
CREATE TEMP TABLE temp_main_full (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE temp_main_full SET(parallel_workers = 32);
INSERT INTO temp_main_full (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT s.submission_author, SUBSTRING(s.submission_id,4),
        s.submission_created_utc, c.author, c.id, c.created_utc, s.subreddit
        FROM comments AS c
        INNER JOIN temp_subms_full AS s
        ON c.link_id = s.submission_id
        WHERE c.author IS NOT NULL
        AND c.author NOT LIKE '[deleted]';
-- Result: 408,925,547 [vs 447,579,856]
SELECT * FROM temp_main_full LIMIT 100;
CREATE INDEX idx_tmf_cid ON temp_main_full (comment_id);
CREATE INDEX idx_tmf_sid ON temp_main_full (submission_id);
DROP TABLE temp_subms_full;
SELECT pg_size_pretty(pg_relation_size('temp_main_full'));
-- Result: 35 [vs 42GB]

-- Add random authors' submissions and comments to table
CREATE TABLE sample_random_20el (
    submission_author VARCHAR(30),
    submission_id VARCHAR(20),
    submission_created_utc INTEGER,
    comment_author VARCHAR(30),
    comment_id VARCHAR(20),
    comment_created_utc INTEGER,
    subreddit VARCHAR(30)
);
ALTER TABLE sample_random_20el SET(parallel_workers = 32);
-- Make sure all random authors' submissions have at least the first comment
WITH auths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_random_20el (
            submission_id,
            submission_author,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id) sm.submission_id,
            sm.submission_author, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, auths
            WHERE sm.submission_author IN (auths.author)
            ORDER BY sm.submission_id, sm.comment_created_utc ASC;
-- Result: 205,183 [vs 229,928]
SELECT * FROM sample_random_20el LIMIT 100;
-- Make sure random authors' first comments on submissions are in the table
WITH auths AS (SELECT DISTINCT ON (temp_auths.author) author FROM temp_auths)
    INSERT INTO sample_random_20el (
            submission_author,
            submission_id,
            submission_created_utc,
            comment_author,
            comment_id,
            comment_created_utc,
            subreddit
        )
        SELECT DISTINCT ON (sm.submission_id, sm.comment_author) sm.submission_author,
            sm.submission_id, sm.submission_created_utc, sm.comment_author,
            sm.comment_id, sm.comment_created_utc, sm.subreddit
            FROM temp_main_full AS sm, auths
            WHERE sm.comment_author IN (auths.author)
            ORDER BY sm.submission_id, sm.comment_author, sm.comment_created_utc ASC;
-- Result: 1,586,458 (vs 1,182,614 from temp_subms_full) [vs 1,879,465]
SELECT * FROM sample_random_20el OFFSET 205175 LIMIT 100;
SELECT COUNT(*) FROM sample_random_20el;
-- Result: 1,791,641 (= 205,183 + 1,586,458) [vs 2,109,393]
-- Sample 1% of other comments on all submissions
INSERT INTO sample_random_20el (
        submission_author,
        submission_id,
        submission_created_utc,
        comment_author,
        comment_id,
        comment_created_utc,
        subreddit
    )
    SELECT sm.submission_author, sm.submission_id, sm.submission_created_utc,
        sm.comment_author, sm.comment_id, sm.comment_created_utc, sm.subreddit
        FROM temp_main_full AS sm
        TABLESAMPLE BERNOULLI (1)
        REPEATABLE (407);
-- Result: 4,088,472 [vs 4,475,200]
SELECT * FROM sample_random_20el OFFSET 1791625 LIMIT 100;
SELECT COUNT(*) FROM sample_random_20el;
-- Result: 5,880,113 (= 1,791,641 + 4,088,472) [vs 6,584,593]
SELECT pg_size_pretty(pg_relation_size('sample_random_20el'));
-- Result: 514MB [vs 577MB]






--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
-- EXPORT SAMPLE TO CSV
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- Export sample_main_20el table to CSV file
COPY (SELECT * FROM sample_main_20el)
TO '/media/seagate0/reddit/samples/sample_main_20el.csv'
    DELIMITER ',' CSV HEADER;

-- Export sample_mentalhealth_20el table to CSV file
COPY (SELECT * FROM sample_mentalhealth_20el)
TO '/media/seagate0/reddit/samples/sample_mentalhealth_20el.csv'
    DELIMITER ',' CSV HEADER;

-- Export sample_selfhelp_20el table to CSV file
COPY (SELECT * FROM sample_selfhelp_20el)
TO '/media/seagate0/reddit/samples/sample_selfhelp_20el.csv'
    DELIMITER ',' CSV HEADER;

-- Export sample_random_20el table to CSV file
COPY (SELECT * FROM sample_random_20el)
TO '/media/seagate0/reddit/samples/sample_random_20el.csv'
    DELIMITER ',' CSV HEADER;
