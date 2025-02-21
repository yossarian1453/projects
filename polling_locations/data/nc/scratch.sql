-- SELECT county, precinct, contest_type, runoff_status
-- FROM analysis.nc_precinct_results_2012
-- GROUP BY 1, 2, 3, 4

/*
WITH subq AS (
	SELECT county, precinct
	FROM analysis.nc_precinct_results_2012
	GROUP BY 1, 2
)
SELECT county, county_name, precinct, precinct_name
FROM subq
LEFT JOIN analysis.nc_polling_places_2012
	ON lower(split_part(precinct,'_', 2)) = lower(precinct_name)
	AND lower(county) = lower(county_name)

WITH subq AS (
	SELECT county, precinct, county_name, precinct_name
	FROM analysis.nc_precinct_results_2012
	LEFT JOIN analysis.nc_polling_places_2012
		ON lower(split_part(precinct,'_', 2)) = lower(precinct_name)
		AND lower(county) = lower(county_name)
	GROUP BY 1, 2, 3, 4
)
SELECT *
FROM subq
WHERE county_name is null
AND precinct NOT in ('ABSENTEE BY MAIL','CURBSIDE','PROVISIONAL','ONE STOP')
;
*/

WITH _subq AS (
    SELECT 
		replace(county_nam,'_',' ') AS county_nam
		, trim(enr_desc) AS enr_desc
		, trim(split_part(enr_desc,'_',1)) AS pct_id
		, trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g')) AS pct_nam
	FROM analysis.nc_2012_pct
)
SELECT 
    name
	, county_name
	, precinct_name
    , county_nam
	, enr_desc
	, pct_id
	, pct_nam
FROM analysis.nc_polling_places_2012 AS places
FULL OUTER JOIN _subq AS shp
	ON county_name = county_nam
		AND (precinct_name = enr_desc
			OR precinct_name = pct_id
			OR precinct_name = pct_nam
		)
WHERE county_name = 'BUNCOMBE' OR county_nam = 'BUNCOMBE'
-- WHERE county_name is null or county_nam is null
AND name <> pct_nam
ORDER BY 1, 2


-- WHERE county_name = 'CUMBERLAND' OR county_nam = 'CUMBERLAND'
-- WHERE county_name is null or county_nam is null

/*
BUNCOMBE
CALDWELL
CARTERET
CUMBERLAND
*/

-- exact number of votes compared to wiki:

SELECT choice, sum(total_votes)
FROM analysis.results_pct_20241105
WHERE choice in ('Donald J. Trump', 'Kamala D. Harris')
GROUP BY 1

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

-- precinct sorted table is the best one to use since it includes provisional ballots with 
WITH subq AS (
	SELECT county, precinct_code
	FROM analysis.statewide_precinct_sort_20241105
	WHERE candidate_name in ('Donald J. Trump')
	AND precinct_name is not null
	GROUP BY 1, 2
)
SELECT count(*), 'results' AS source_table
FROM subq

UNION ALL

SELECT count(*), 'polling places' AS source_table
FROM analysis.nc_polling_places_2024

UNION ALL 

SELECT count(*), 'precincts' AS source_table
FROM analysis.sbe_precincts_20240723

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

WITH subq AS (
	SELECT county, precinct_code, precinct_name
	FROM analysis.statewide_precinct_sort_20241105
	WHERE candidate_name in ('Donald J. Trump')
	AND precinct_name is not null
	GROUP BY 1, 2, 3
)
SELECT count(*), 'results' AS source_table
FROM subq



------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

SELECT *
-- prec_id, enr_desc, county_nam, county_id, g16prertru, g16predcli, g16preljoh, g16preowri
FROM analysis.nc_2016
LIMIT 100;


SELECT *
FROM analysis.nc_polling_places_2016
LIMIT 100;


SELECT county, precinct_code, candidate_name, sum(vote_ct)
FROM analysis.statewide_precinct_sort_20241105
WHERE county = 'WAKE' 
AND contest_title = 'US PRESIDENT'
AND candidate_name in ('Donald J. Trump', 'Kamala D. Harris')
AND precinct_code like '07-07%'
GROUP BY 1, 2, 3


SELECT *
FROM analysis.statewide_precinct_sort_20241105
WHERE candidate_name in ('Donald J. Trump')
-- AND county = 'WAKE'
-- ORDER BY precinct_code
LIMIT 100


SELECT prec_id, enr_desc, county_nam
FROM analysis.sbe_precincts_20240723
WHERE county_nam = 'WAKE'
ORDER BY enr_desc
LIMIT 1000


WITH results_subq AS (
	SELECT county, precinct
	FROM analysis.nc_precinct_results_2012
	WHERE precinct not in ('ABSENTEE BY MAIL','CURBSIDE','PROVISIONAL')
	AND contest_type <> 'C'
	GROUP BY 1, 2
)
SELECT county, precinct, county_nam, enr_desc
FROM results_subq
FULL OUTER JOIN analysis.nc_2012_pct
	ON county = county_nam
	AND precinct = replace(enr_desc,'#','')
-- WHERE county = 'CUMBERLAND' OR county_nam = 'CUMBERLAND'
-- AND county is null or county_nam is null
order by 1, 2, 3, 4
;
-- "G11A-1_SPRING LAKE-1-G11"
-- "G11A-2_SPRING LAKE-2-G11"


WITH _results AS (
	SELECT county, precinct
	FROM analysis.results_pct_20241105
	WHERE real_precinct = 'Y'
	GROUP BY 1, 2
)

SELECT county_nam, prec_id, enr_desc, county, precinct
FROM analysis.sbe_precincts_20240723
FULL OUTER JOIN _results
ON county_nam = county
AND (precinct = prec_id OR precinct = enr_desc)
-- WHERE county_nam is null OR county is null
WHERE county_nam = 'WAKE' OR county = 'WAKE'
ORDER BY 1, 2, 3, 4, 5
;

SELECT *
FROM analysis.results_pct_20241105
WHERE county = 'WAKE' 
AND contest_name = 'US PRESIDENT'
AND choice in ('Donald J. Trump', 'Kamala D. Harris')
AND precinct like '07-07%'
ORDER BY precinct

"01-07A"
"07-07A"

------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------

