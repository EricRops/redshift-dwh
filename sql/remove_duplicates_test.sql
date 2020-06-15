BEGIN;

-- First identify all the rows that are duplicate
CREATE TEMP TABLE duplicate_ids AS
SELECT songplay_id
FROM songplays
GROUP BY songplay_id
HAVING COUNT(*) > 1;

-- Extract one copy of all the duplicate rows
CREATE TEMP TABLE new_table(LIKE songplays);

INSERT INTO new_table
SELECT DISTINCT *
FROM songplays
WHERE songplay_id IN(
     SELECT songplay_id 
     FROM duplicate_ids
);

-- Remove all rows that were duplicated (all copies).
DELETE FROM songplays
WHERE songplay_id IN(
     SELECT songplay_id 
     FROM duplicate_ids
);

-- Insert back in the single copies
INSERT INTO songplays
SELECT *
FROM new_table;

-- Cleanup
DROP TABLE duplicate_ids;
DROP TABLE new_table;

COMMIT;