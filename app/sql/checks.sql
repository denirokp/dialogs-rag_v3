-- Evidence-100
SELECT SUM(CASE WHEN text_quote IS NULL OR LENGTH(text_quote)=0 THEN 1 ELSE 0 END)=0 AS evidence_100 FROM mentions;

-- Client-only-100: проверяем, что turn_id и dialog_id из client-реплик (требует joined данных, пропустим здесь)

-- Dedup ≤1%: считаем дубликаты text_quote внутри (dialog_id, theme)
WITH t AS (
  SELECT dialog_id, theme, text_quote, COUNT(*) AS c
  FROM mentions GROUP BY 1,2,3
),
agg AS (
  SELECT SUM(c) AS total, SUM(CASE WHEN c>1 THEN c-1 ELSE 0 END) AS dups FROM t
)
SELECT (CAST(dups AS DOUBLE)/NULLIF(total,0)) AS dedup_rate FROM agg;
