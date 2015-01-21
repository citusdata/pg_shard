-- ===================================================================
-- test end-to-end query functionality
-- ===================================================================

CREATE TABLE articles (
	id bigint NOT NULL,
	author_id bigint NOT NULL,
	title text NOT NULL,
	word_count integer NOT NULL CHECK (word_count > 0)
);

SELECT master_create_distributed_table('articles', 'author_id');

-- test when a table is distributed but no shards created yet
SELECT count(*) from articles;

\set VERBOSITY terse
SELECT master_create_worker_shards('articles', 2, 1);
\set VERBOSITY default

INSERT INTO articles VALUES ( 1,  1, 'arsenous', 9572);
INSERT INTO articles VALUES ( 2,  2, 'abducing', 13642);
INSERT INTO articles VALUES ( 3,  3, 'asternal', 10480);
INSERT INTO articles VALUES ( 4,  4, 'altdorfer', 14551);
INSERT INTO articles VALUES ( 5,  5, 'aruru', 11389);
INSERT INTO articles VALUES ( 6,  6, 'atlases', 15459);
INSERT INTO articles VALUES ( 7,  7, 'aseptic', 12298);
INSERT INTO articles VALUES ( 8,  8, 'agatized', 16368);
INSERT INTO articles VALUES ( 9,  9, 'alligate', 438);
INSERT INTO articles VALUES (10, 10, 'aggrandize', 17277);
INSERT INTO articles VALUES (11,  1, 'alamo', 1347);
INSERT INTO articles VALUES (12,  2, 'archiblast', 18185);
INSERT INTO articles VALUES (13,  3, 'aseyev', 2255);
INSERT INTO articles VALUES (14,  4, 'andesite', 19094);
INSERT INTO articles VALUES (15,  5, 'adversa', 3164);
INSERT INTO articles VALUES (16,  6, 'allonym', 2);
INSERT INTO articles VALUES (17,  7, 'auriga', 4073);
INSERT INTO articles VALUES (18,  8, 'assembly', 911);
INSERT INTO articles VALUES (19,  9, 'aubergiste', 4981);
INSERT INTO articles VALUES (20, 10, 'absentness', 1820);
INSERT INTO articles VALUES (21,  1, 'arcading', 5890);
INSERT INTO articles VALUES (22,  2, 'antipope', 2728);
INSERT INTO articles VALUES (23,  3, 'abhorring', 6799);
INSERT INTO articles VALUES (24,  4, 'audacious', 3637);
INSERT INTO articles VALUES (25,  5, 'antehall', 7707);
INSERT INTO articles VALUES (26,  6, 'abington', 4545);
INSERT INTO articles VALUES (27,  7, 'arsenous', 8616);
INSERT INTO articles VALUES (28,  8, 'aerophyte', 5454);
INSERT INTO articles VALUES (29,  9, 'amateur', 9524);
INSERT INTO articles VALUES (30, 10, 'andelee', 6363);
INSERT INTO articles VALUES (31,  1, 'athwartships', 7271);
INSERT INTO articles VALUES (32,  2, 'amazon', 11342);
INSERT INTO articles VALUES (33,  3, 'autochrome', 8180);
INSERT INTO articles VALUES (34,  4, 'amnestied', 12250);
INSERT INTO articles VALUES (35,  5, 'aminate', 9089);
INSERT INTO articles VALUES (36,  6, 'ablation', 13159);
INSERT INTO articles VALUES (37,  7, 'archduchies', 9997);
INSERT INTO articles VALUES (38,  8, 'anatine', 14067);
INSERT INTO articles VALUES (39,  9, 'anchises', 10906);
INSERT INTO articles VALUES (40, 10, 'attemper', 14976);
INSERT INTO articles VALUES (41,  1, 'aznavour', 11814);
INSERT INTO articles VALUES (42,  2, 'ausable', 15885);
INSERT INTO articles VALUES (43,  3, 'affixal', 12723);
INSERT INTO articles VALUES (44,  4, 'anteport', 16793);
INSERT INTO articles VALUES (45,  5, 'afrasia', 864);
INSERT INTO articles VALUES (46,  6, 'atlanta', 17702);
INSERT INTO articles VALUES (47,  7, 'abeyance', 1772);
INSERT INTO articles VALUES (48,  8, 'alkylic', 18610);
INSERT INTO articles VALUES (49,  9, 'anyone', 2681);
INSERT INTO articles VALUES (50, 10, 'anjanette', 19519);

-- first, test zero-shard query
SELECT COUNT(*) FROM articles WHERE author_id = 1 AND author_id = 2;

-- single-shard tests

-- test simple select for a single row
SELECT * FROM articles WHERE author_id = 10 AND id = 50;

-- get all titles by a single author
SELECT title FROM articles WHERE author_id = 10;

-- try ordering them by word count
SELECT title, word_count FROM articles
	WHERE author_id = 10
	ORDER BY word_count DESC NULLS LAST;

-- look at last two articles by an author
SELECT title, id FROM articles
	WHERE author_id = 5
	ORDER BY id
	LIMIT 2;

-- find all articles by two authors in same shard
SELECT title, author_id FROM articles
	WHERE author_id = 7 OR author_id = 8
	ORDER BY author_id ASC, id;

-- add in some grouping expressions, still on same shard
SELECT author_id, sum(word_count) AS corpus_size FROM articles
	WHERE author_id = 1 OR author_id = 7 OR author_id = 8 OR author_id = 10
	GROUP BY author_id
	HAVING sum(word_count) > 40000
	ORDER BY sum(word_count) DESC;

-- UNION/INTERSECT queries are unsupported
SELECT * FROM articles WHERE author_id = 10 UNION
SELECT * FROM articles WHERE author_id = 1; 

-- queries using CTEs are unsupported
CREATE TABLE authors ( name text, id bigint );

WITH long_names AS ( SELECT id FROM authors WHERE char_length(name) > 15 )
SELECT title FROM articles;

--  queries which involve functions in FROM clause are unsupported.
SELECT * FROM articles, position('om' in 'Thomas');

-- subqueries are not supported in WHERE clause
SELECT * FROM articles WHERE author_id IN (SELECT id FROM authors WHERE name LIKE '%a');

-- subqueries are not supported in FROM clause
SELECT articles.id,test.word_count FROM articles, (SELECT id, word_count FROM articles) AS test where test.id = articles.id;

-- subqueries are not supported in SELECT clause
SELECT  a.title AS  name,(SELECT a2.id FROM authors a2 WHERE a.id = a2.id  LIMIT 1) AS special_price FROM articles a;

-- joins are not supported in WHERE clause
SELECT title, authors.name FROM authors, articles WHERE authors.id = articles.author_id;

-- joins are not supported in FROM clause
SELECT * FROM  (articles INNER JOIN authors ON articles.id = authors.id);

-- test cross-shard queries
SELECT COUNT(*) FROM articles;

-- try query with more SQL features
SELECT author_id, sum(word_count) AS corpus_size FROM articles
	GROUP BY author_id
	HAVING sum(word_count) > 25000
	ORDER BY sum(word_count) DESC
	LIMIT 5;

-- verify pg_shard produces correct remote SQL
SET pg_shard.log_distributed_statements = on;
SET client_min_messages = log;

SELECT count(*) FROM articles WHERE word_count > 10000;

SET client_min_messages = DEFAULT;
SET pg_shard.log_distributed_statements = DEFAULT;

-- use HAVING without its variable in target list
SELECT author_id FROM articles
	GROUP BY author_id
	HAVING sum(word_count) > 50000
	ORDER BY author_id;

-- verify temp tables used by cross-shard queries do not persist
SELECT COUNT(*) FROM pg_class WHERE relname LIKE 'pg_shard_temp_table%' AND
									relkind = 'r';

