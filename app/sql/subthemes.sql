-- summary_subthemes
WITH base AS (
  SELECT dialog_id, theme, COALESCE(subtheme,'') AS subtheme FROM mentions
  WHERE theme IS NOT NULL AND theme <> ''
)
SELECT
  theme, subtheme,
  COUNT(DISTINCT dialog_id) AS dialogov,
  COUNT(*) AS upominanii,
  ROUND(100.0 * COUNT(DISTINCT dialog_id) / (SELECT COUNT(DISTINCT dialog_id) FROM mentions), 1) AS share_dialogs_pct
FROM base
GROUP BY 1,2
ORDER BY dialogov DESC;
