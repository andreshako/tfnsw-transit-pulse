-- Singular test: fails (returns a row) if the mart is empty.
--
-- Every existing schema test on fct_line_otp_daily (not_null,
-- accepted_values, accepted_range, unique_combination_of_columns) passes
-- vacuously on zero rows, so a broken upstream join -- e.g. the route seed
-- no longer matching any staging row -- can silently build an empty table
-- and still show a fully green `dbt build`. This is a direct fix for that
-- gap (see LEARNING_NOTES section 12): it happened for real, in CI, before
-- this test existed.

select 1 as failing_check
from (select count(*) as row_count from {{ ref('fct_line_otp_daily') }})
where row_count = 0
