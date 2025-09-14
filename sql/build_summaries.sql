-- expects tables: dialogs(dialog_id), mentions(json lines ingested to table mentions with columns projected)

-- THEMES
CREATE OR REPLACE TABLE summary_themes AS
SELECT theme,
       COUNT(DISTINCT dialog_id) AS dialog_count,
       COUNT(*) AS mention_count,
       ROUND(100.0 * COUNT(DISTINCT dialog_id) / (SELECT COUNT(*) FROM dialogs), 1) AS share_of_dialogs_pct
FROM mentions
GROUP BY theme
ORDER BY dialog_count DESC;

-- SUBTHEMES
CREATE OR REPLACE TABLE summary_subthemes AS
SELECT theme, subtheme,
       COUNT(DISTINCT dialog_id) AS dialog_count,
       COUNT(*) AS mention_count,
       ROUND(100.0 * COUNT(DISTINCT dialog_id) / (SELECT COUNT(*) FROM dialogs), 1) AS share_of_dialogs_pct
FROM mentions
GROUP BY theme, subtheme
ORDER BY dialog_count DESC;

-- QUOTES INDEX
CREATE OR REPLACE TABLE index_quotes AS
SELECT theme, subtheme, dialog_id, turn_id, text_quote
FROM mentions;
