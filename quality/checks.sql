-- Q1 Evidence-100
SELECT COUNT(*) AS empty_quotes
FROM mentions
WHERE text_quote IS NULL OR LENGTH(TRIM(text_quote)) = 0;

-- Q2 Client-only-100
SELECT COUNT(*) AS non_client_mentions
FROM mentions m
LEFT JOIN utterances u USING (dialog_id, turn_id)
WHERE u.role <> 'client';

-- Q3 Dedup ≤1% (exact norm)
WITH norm AS (
  SELECT dialog_id, subtheme, LOWER(REGEXP_REPLACE(text_quote, '\\s+', ' ')) AS qn
  FROM mentions
),
dups AS (
  SELECT dialog_id, subtheme, qn, COUNT(*) AS c
  FROM norm GROUP BY 1,2,3 HAVING COUNT(*) > 1
)
SELECT ROUND(100.0 * COALESCE(SUM(c - 1),0) / (SELECT COUNT(*) FROM mentions), 2) AS dup_pct;

-- Q4 Coverage ≥98%
SELECT ROUND(100.0 * COUNT(*) FILTER (WHERE theme='прочее') / COUNT(*), 2) AS misc_share_pct
FROM mentions;

-- Ambiguity report (share confidence < 0.6)
SELECT theme, subtheme,
       ROUND(100.0 * AVG((confidence < 0.6)::INT), 2) AS low_conf_pct,
       COUNT(*) AS cnt
FROM mentions
GROUP BY theme, subtheme
ORDER BY low_conf_pct DESC, cnt DESC;
