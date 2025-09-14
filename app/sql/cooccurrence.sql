-- co-occurrence (по диалогу)
WITH per_dialog AS (
  SELECT dialog_id, LIST(DISTINCT theme) AS themes
  FROM mentions
  WHERE theme IS NOT NULL AND theme <> ''
  GROUP BY 1
)
SELECT a.theme AS theme_a, b.theme AS theme_b, COUNT(*) AS cnt
FROM per_dialog, UNNEST(themes) a(theme), UNNEST(themes) b(theme)
WHERE a.theme < b.theme
GROUP BY 1,2
ORDER BY cnt DESC;
