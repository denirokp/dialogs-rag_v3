-- summary_themes
WITH m AS (
  SELECT dialog_id, theme FROM mentions
  WHERE theme IS NOT NULL AND theme <> ''
  GROUP BY ALL
),
mc AS (
  SELECT theme, COUNT(*) AS mention_count FROM mentions GROUP BY 1
),
dc AS (
  SELECT theme, COUNT(DISTINCT dialog_id) AS dialog_count FROM mentions GROUP BY 1
)
SELECT
  m.theme,
  dc.dialog_count AS dialogov,
  mc.mention_count AS upominanii,
  ROUND(100.0 * dc.dialog_count / (SELECT COUNT(DISTINCT dialog_id) FROM mentions), 1) AS share_dialogs_pct
FROM dc
JOIN mc USING(theme)
ORDER BY dialogov DESC;
