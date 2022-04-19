SELECT
  issues.error_code,
  issues.description,
  issues.release_note_type,
  issues.product_name,
  issues.published_at
FROM (
  SELECT
    *
  FROM
    `bigquery-public-data.google_cloud_release_notes.release_notes`,
    UNNEST(REGEXP_EXTRACT_ALL(description, r"\[(.*?)\]")) AS error_code
  WHERE
    release_note_type = 'SECURITY_BULLETIN'
    AND description LIKE "%security vulner%"
    AND description NOT LIKE "%fixes%"
    AND error_code LIKE '%CVE%'
    AND (EXTRACT(QUARTER
      FROM
        published_at) = EXTRACT(QUARTER
      FROM
        CURRENT_DATE)
      OR EXTRACT(QUARTER
      FROM
        published_at) -1 = EXTRACT(QUARTER
      FROM
        CURRENT_DATE) - 1)
    AND EXTRACT(YEAR
    FROM
      published_at) = EXTRACT(YEAR
    FROM
      CURRENT_DATE)) issues
LEFT JOIN (
  SELECT
    *
  FROM
    `bigquery-public-data.google_cloud_release_notes.release_notes`,
    UNNEST(REGEXP_EXTRACT_ALL(description, r"\[(.*?)\]")) AS error_code
  WHERE
    (description LIKE "%fixed%"
      OR description LIKE "%fixes%")
    AND error_code LIKE "%CVE%") fixes
ON
  issues.error_code = fixes.error_code
WHERE
  fixes.release_note_type IS NULL
