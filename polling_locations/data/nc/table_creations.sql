DROP TABLE analysis.nc_pct_shape_results_polling_locs_2016;
CREATE TABLE analysis.nc_pct_shape_results_polling_locs_2016 AS (
    WITH _nc_2016 AS (
        SELECT 
            geom,
            g16prertru,
            g16predcli,
            g16preljoh,
            g16preowri,
            g16prertru + g16predcli + g16preljoh + g16preowri AS g16_total_votes,
            replace(county_nam, '_', ' ') AS county_nam,
            trim(enr_desc) AS enr_desc,
            prec_id,
            trim(split_part(enr_desc, '_', 1)) AS pct_id,
            trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')) AS clean_enr_desc,
            trim(REPLACE(enr_desc, ' ', '')) AS no_space_enr_desc,

            CASE 
                WHEN county_nam IN ('GREENE','HALIFAX','MCDOWELL','NORTHAMPTON','PAMLICO') THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G5A_AUMAN-G5A-1' THEN 'AUMAN-1-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G5A_AUMAN-G5A-2' THEN 'AUMAN-2-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G2C_CROSS CREEK #23-G2C-1' THEN 'CROSS CREEK 23-1-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G2C_CROSS CREEK #23-G2C-2' THEN 'CROSS CREEK 23-2-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G5B_CROSS CREEK #28-G5B-1' THEN 'CROSS CREEK 28-1-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G5B_CROSS CREEK #28-G5B-2' THEN 'CROSS CREEK 28-2-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G9B_HOPE MILLS #2-G9B-1' THEN 'HOPE MILLS 2A-G9'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G9B_HOPE MILLS #2-G9B-2' THEN 'HOPE MILLS 2B-G9'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G3A_PEARCES MILL #2-G3A-1' THEN 'PEARCES MILL 2A-G3'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G3A_PEARCES MILL #2-G3A-2' THEN 'PEARCES MILL 2B-G3'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G11A-1_SPRING LAKE-G11' THEN 'SPRING LAKE-1-G11'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G11A-2_SPRING LAKE-G11' THEN 'SPRING LAKE-2-G11'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G2E_WESTAREA-G2E-1' THEN 'WESTAREA-1-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'G2E_WESTAREA-G2E-2' THEN 'WESTAREA-2-G2'
                WHEN county_nam = 'CUMBERLAND' 
                    THEN replace(
                        TRIM(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+_', '', 'g'), 
                                    '_', ' '), 
                                '#', '', 'g')
                            ),
                        '- ','-')
                WHEN county_nam = 'CALDWELL' AND enr_desc = 'PR07_GLOBE/JOHNS RIVER/MULBERRY/WILSON CREEK' THEN 'GLOBE/JOHNS RIVER/MULBERRY/WIL'
                WHEN county_nam = 'CARTERET' AND enr_desc = 'ATSL_ATLANTIC/ SEA LEVEL' THEN 'ATLANTIC/SEA LEVEL'
                WHEN county_nam = 'CASWELL' AND enr_desc = 'YAN2_YANCEYVILLE 2' THEN 'YANCEYVILLE'
                WHEN county_nam = 'BUNCOMBE' AND enr_desc LIKE '% HIGH SCHOOL' THEN REPLACE(enr_desc, ' HIGH SCHOOL', '')
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'BETHWR' THEN 'BETHWARE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'FALSTN' THEN 'FALLSTON'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KINGST' THEN 'KINGSTOWN'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KM N' THEN 'KINGS MOUNTAIN NORTH'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KM S' THEN 'KINGS MOUNTAIN SOUTH'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'LATT' THEN 'LATTIMORE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'LAWNDL' THEN 'LAWNDALE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'MRB-YO' THEN 'MOORESBORO-YOUNG'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'OAKGRV' THEN 'OAK GROVE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'POLKVL' THEN 'POLKVILLE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'SHANGI' THEN 'SHANGHAI'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'Shelby 4' THEN 'SHELBY #4'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'S 5' THEN 'SHELBY #5'
                
                WHEN county_nam = 'LEE' THEN concat('PRECINCT ', enr_desc)
                
                WHEN county_nam = 'MADISON' AND enr_desc = 'EBBS C_HAPEL' THEN 'EBBS CHAPEL'
                WHEN county_nam = 'MADISON' AND enr_desc = 'GRAPEV_INE' THEN 'GRAPEVINE'
                WHEN county_nam = 'MADISON' AND enr_desc = 'HOT SP_RINGS' THEN 'HOT SPRINGS'
                WHEN county_nam = 'MADISON' AND enr_desc = 'MARS H_ILL' THEN 'MARS HILL'
                WHEN county_nam = 'MADISON' AND enr_desc = 'REVERE_RICE COVE' THEN 'REVERE-RICE COVE'
                WHEN county_nam = 'MADISON' THEN replace(enr_desc,'_',' ')

                WHEN county_nam = 'MARTIN' AND enr_desc = 'GN_GOOSENEST' THEN 'GOOSE NEST'

                WHEN county_nam = 'MECKLENBURG' AND enr_desc like '%.%' THEN concat('PCT ', split_part(enr_desc,'.',1))
                WHEN county_nam = 'MECKLENBURG' THEN concat('PCT ', enr_desc)

                WHEN county_nam = 'PASQUOTANK' AND split_part(enr_desc,'_',1) IN ('NORTH','SOUTH','EAST','WEST') THEN replace(enr_desc,'_',' ')

                WHEN county_nam = 'PERQUIMANS' THEN replace(enr_desc,'_','')

                WHEN county_nam = 'PERSON' THEN upper(split_part(enr_desc,'_',2))

                WHEN county_nam = 'PITT' AND enr_desc = '1513A_GREENVILE #13A' THEN 'GREENVILLE #13A'

                WHEN county_nam = 'WAKE' THEN concat('PRECINCT ', enr_desc)

                WHEN county_nam = 'YANCEY' THEN REPLACE(REGEXP_REPLACE(enr_desc, '^[^ ]+ ', '', 'g'),'_', '')
                
                WHEN county_nam = 'ROBESON' AND enr_desc = '11A_LUMBERTON #1' THEN 'LUMBERTON #11A'

                ELSE trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                
            END AS fixed_precinct_name

        FROM analysis.nc_2016
    )

    SELECT 
        geom,
        g16prertru,
        g16predcli,
        g16preljoh,
        g16preowri,
        g16_total_votes,
        g16prertru - g16predcli AS g16prertru_margin_vote,
        g16predcli - g16prertru AS g16predcli_margin_vote,
        (g16prertru/g16_total_votes) - (g16predcli/g16_total_votes) AS g16prertru_margin_pct,
        (g16predcli/g16_total_votes) - (g16prertru/g16_total_votes) AS g16predcli_margin_pct,
        nc.county_nam,
        nc.enr_desc,
        nc.pct_id,
        nc.prec_id,
        nc.clean_enr_desc,
        nc.no_space_enr_desc,
        CASE WHEN nc.fixed_precinct_name = '' THEN NULL ELSE nc.fixed_precinct_name END AS fixed_precinct_name, -- ✅ Includes **ALL** fixes
        pp.county_name,
        pp.precinct_name,
        pp.polling_place_id,
        pp.polling_place_name
    FROM _nc_2016 AS nc
    FULL OUTER JOIN analysis.nc_polling_places_2016 AS pp
    ON nc.county_nam = pp.county_name
    AND (
            nc.enr_desc = pp.precinct_name
            OR nc.fixed_precinct_name = pp.precinct_name
            OR nc.clean_enr_desc = pp.precinct_name
            OR nc.no_space_enr_desc = pp.precinct_name
            OR LOWER(nc.clean_enr_desc) = LOWER(pp.precinct_name)
            OR nc.pct_id = pp.precinct_name
            OR nc.prec_id = pp.precinct_name
    )
    -- WHERE (nc.county_nam IS NULL OR pp.county_name IS NULL)
    -- WHERE (nc.county_nam = 'PASQUOTANK' OR pp.county_name = 'PASQUOTANK') AND (nc.county_nam IS NULL OR pp.county_name IS NULL)
    -- WHERE (nc.county_nam = 'ROBESON' OR pp.county_name = 'ROBESON') AND (enr_desc LIKE '%LUMBERTON%' OR precinct_name LIKE '%LUMBERTON%')
    -- WHERE (nc.county_nam = 'CARTERET' OR pp.county_name = 'CARTERET')
    -- WHERE enr_desc LIKE '%/%' OR precinct_name LIKE '%/%'
    ORDER BY nc.county_nam, nc.enr_desc, pp.county_name, pp.precinct_name
);

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

-- data dictionary: https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/layout_voter_stats.txt 
DROP TABLE analysis.nc_pct_shape_results_polling_locs_voter_reg_2016;
CREATE TABLE analysis.nc_pct_shape_results_polling_locs_voter_reg_2016 AS (
    WITH voter_reg AS (
        SELECT
            county_desc, 
            precinct_abbrv, 
            sum(total_voters) AS total_voters, 
            SUM(total_voters) FILTER (WHERE party_cd = 'DEM') AS total_dem_voters,
            SUM(total_voters) FILTER (WHERE party_cd = 'REP') AS total_gop_voters,
            SUM(total_voters) FILTER (WHERE party_cd = 'LIB') AS total_lib_voters,
            SUM(total_voters) FILTER (WHERE party_cd = 'UNA') AS total_una_voters,
            SUM(total_voters) FILTER (WHERE party_cd = 'GRE') AS total_grn_voters,
            SUM(total_voters) FILTER (WHERE party_cd = 'CST') AS total_const_voters,
            SUM(total_voters) FILTER (WHERE party_cd = 'NLB') AS total_nlb_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'A') AS total_asian_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'B') AS total_black_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'I') AS total_iaan_voters, 
            SUM(total_voters) FILTER (WHERE race_code = 'M') AS total_tomr_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'O') AS total_other_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'P') AS total_nhpi_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'U') AS total_undr_voters,
            SUM(total_voters) FILTER (WHERE race_code = 'W') AS total_white_voters,
            SUM(total_voters) FILTER (WHERE ethnic_code = 'HL') AS total_hl_voters,
            SUM(total_voters) FILTER (WHERE ethnic_code = 'NL') AS total_nhl_voters,
            SUM(total_voters) FILTER (WHERE ethnic_code = 'UN') AS total_undh_voters,
            SUM(total_voters) FILTER (WHERE sex_code = 'F') AS total_fem_voters,
            SUM(total_voters) FILTER (WHERE sex_code = 'M') AS total_male_voters,
            SUM(total_voters) FILTER (WHERE sex_code = 'U') AS total_undg_voters,
            SUM(total_voters) FILTER (WHERE age = 'Age 18 - 25') AS total_18_25_voters,
            SUM(total_voters) FILTER (WHERE age = 'Age 26 - 40') AS total_26_40_voters,
            SUM(total_voters) FILTER (WHERE age = 'Age 41 - 65') AS total_41_65_voters,
            SUM(total_voters) FILTER (WHERE age = 'Age Over 66') AS total_65plus_voters
        FROM analysis.nc_voter_stats_2016
        WHERE precinct_abbrv <> ' '
        GROUP BY county_desc, precinct_abbrv
    )
    
    SELECT 
        psrpl.*,
        vr.*,
        CAST(total_dem_voters AS FLOAT) / NULLIF(total_voters, 0) AS dem_pct,
        CAST(total_gop_voters AS FLOAT) / NULLIF(total_voters, 0) AS gop_pct,
        CAST(total_lib_voters AS FLOAT) / NULLIF(total_voters, 0) AS lib_pct,
        CAST(total_una_voters AS FLOAT) / NULLIF(total_voters, 0) AS una_pct,
        CAST(total_grn_voters AS FLOAT) / NULLIF(total_voters, 0) AS grn_pct,
        CAST(total_const_voters AS FLOAT) / NULLIF(total_voters, 0) AS const_pct,
        CAST(total_nlb_voters AS FLOAT) / NULLIF(total_voters, 0) AS nlb_pct,
        CAST(total_asian_voters AS FLOAT) / NULLIF(total_voters, 0) AS asian_pct,
        CAST(total_black_voters AS FLOAT) / NULLIF(total_voters, 0) AS black_pct,
        CAST(total_iaan_voters AS FLOAT) / NULLIF(total_voters, 0) AS iian_pct, 
        CAST(total_tomr_voters AS FLOAT) / NULLIF(total_voters, 0) AS tomr_pct,
        CAST(total_other_voters AS FLOAT) / NULLIF(total_voters, 0) AS other_pct,
        CAST(total_nhpi_voters AS FLOAT) / NULLIF(total_voters, 0) AS nhpi_pct,
        CAST(total_undr_voters AS FLOAT) / NULLIF(total_voters, 0) AS undr_pct,
        CAST(total_white_voters AS FLOAT) / NULLIF(total_voters, 0) AS white_pct,
        CAST(total_hl_voters AS FLOAT) / NULLIF(total_voters, 0) AS hl_pct,
        CAST(total_nhl_voters AS FLOAT) / NULLIF(total_voters, 0) AS nhl_pct,
        CAST(total_undh_voters AS FLOAT) / NULLIF(total_voters, 0) AS undh_pct,
        CAST(total_fem_voters AS FLOAT) / NULLIF(total_voters, 0) AS fem_pct,
        CAST(total_male_voters AS FLOAT) / NULLIF(total_voters, 0) AS male_pct,
        CAST(total_undg_voters AS FLOAT) / NULLIF(total_voters, 0) AS undg_pct,
        CAST(total_18_25_voters AS FLOAT) / NULLIF(total_voters, 0) AS "18_25_pct",
        CAST(total_26_40_voters AS FLOAT) / NULLIF(total_voters, 0) AS "26_40_pct",
        CAST(total_41_65_voters AS FLOAT) / NULLIF(total_voters, 0) AS "41_65_pct",
        CAST(total_65plus_voters AS FLOAT) / NULLIF(total_voters, 0) AS "65plus_pct"
    FROM analysis.nc_pct_shape_results_polling_locs_2016 AS psrpl
    FULL OUTER JOIN voter_reg AS vr
    ON county_name = county_desc
        AND (
            enr_desc = precinct_abbrv
            OR prec_id = precinct_abbrv
        )
    ORDER BY county_nam, enr_desc, county_desc, precinct_abbrv
);


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_pct_shape_results_polling_locs_2020;
CREATE TABLE analysis.nc_pct_shape_results_polling_locs_2020 AS (
    WITH _nc_2020 AS (
        SELECT 
            geom,
			g20prertru,
			g20predbid,
			g20preljor,
			g20preghaw,
			g20precbla,
			g20preowri,
            g20prertru + g20predbid + g20preljor + g20preghaw + g20precbla + g20preowri AS g20_total_votes,
            replace(county_nam, '_', ' ') AS county_nam,
            trim(enr_desc) AS enr_desc,
            prec_id,
            trim(split_part(enr_desc, '_', 1)) AS pct_id,
            trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')) AS clean_enr_desc,
            trim(REPLACE(enr_desc, ' ', '')) AS no_space_enr_desc,

            CASE 
                WHEN county_nam = 'ALEXANDER' THEN replace(enr_desc,' ',' #')			
                WHEN county_nam = 'AVERY' THEN replace(enr_desc,'   ',' # ') 
                
                WHEN county_nam = 'BLADEN' THEN replace(enr_desc,'  ',' #')
                WHEN county_nam = 'BRUNSWICK' AND enr_desc = '04C1_04B1' THEN 'BELVILLE 2'
                WHEN county_nam = 'BUNCOMBE' AND enr_desc LIKE '% HIGH SCHOOL' THEN REPLACE(enr_desc, ' HIGH SCHOOL', '')
                WHEN county_nam = 'BURKE' AND enr_desc LIKE '%1-%' THEN REGEXP_REPLACE(enr_desc, '([ ]+)(\d)', ' #\2')
                
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'AUMAN-G5A-1' THEN 'AUMAN-1-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'AUMAN-G5A-2' THEN 'AUMAN-2-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CLIFFDALE_WEST- 1-CL57' THEN 'CLIFFDALE WEST-1-CL57'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CLIFFDALE_WEST- 2-CL57' THEN 'CLIFFDALE WEST-2-CL57'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK 23-G2C-1' THEN 'CROSS CREEK 23-1-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK G2C-2' THEN 'CROSS CREEK 23-2-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK 28-G5B-1' THEN 'CROSS CREEK 28-1-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK 28-G5B-2' THEN 'CROSS CREEK 28-2-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'HOPE MILLS 2-G9B-1' THEN 'HOPE MILLS 2A-G9'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'HOPE MILLS 2-G9B-2' THEN 'HOPE MILLS 2B-G9'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'PEARCES MILL 2-G3A-1' THEN 'PEARCES MILL 2A-G3'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'PEARCES MILL 2-G3A-2' THEN 'PEARCES MILL 2B-G3'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'WESTAREA-G2E-1' THEN 'WESTAREA-1-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'WESTAREA-G2E-2' THEN 'WESTAREA-2-G2'

                WHEN county_nam = 'CALDWELL' AND enr_desc = 'GLOBE/JOHNS RIVER/MULBERRY/WILSON CREEK' THEN 'GLOBE/JOHNS RIVER/MULBERRY/WIL'
                WHEN county_nam = 'CARTERET' AND enr_desc = 'Atlantic/Sea Level/Cedar Island' THEN 'ATLANTIC/SEA LEVEL/CEDAR ISLAN'
                WHEN county_nam = 'CARTERET' AND enr_desc = 'Indian Beach/Salter Path/Pine Knoll' THEN 'INDIAN BEACH/SALTER PATH/PINE '
                WHEN county_nam = 'CARTERET' AND enr_desc = 'OTWAY/STRAITS/BETTIE/Gloucester' THEN 'OTWAY/STRAITS/BETTIE/GLOUCESTE'
                WHEN county_nam = 'CASWELL' AND enr_desc = 'YANCEYVILLE 2' THEN 'YANCEYVILLE'
                WHEN county_nam = 'CATAWBA' THEN REGEXP_REPLACE(enr_desc, '([ ]+)(\d)', ' #\2')
                WHEN county_nam = 'CRAVEN' AND enr_desc = 'DOVER-FORT BARNWELL West' THEN 'DOVER-FORT BARNWELL'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'BETHWR' THEN 'BETHWARE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'FALSTN' THEN 'FALLSTON'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KINGST' THEN 'KINGSTOWN'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KM N' THEN 'KINGS MOUNTAIN NORTH'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KM S' THEN 'KINGS MOUNTAIN SOUTH'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'LATT' THEN 'LATTIMORE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'LAWNDL' THEN 'LAWNDALE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'MRB-YO' THEN 'MOORESBORO-YOUNG'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'OAKGRV' THEN 'OAK GROVE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'POLKVL' THEN 'POLKVILLE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'SHANGI' THEN 'SHANGHAI'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'Shelby 4' THEN 'SHELBY #4'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'S 5' THEN 'SHELBY #5'

                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'COTTON GROVE 10' THEN 'COTTON GROVE  #10'
                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'EMMONS 14' THEN 'EMMONS # 14'
                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'GUMTREE 16' THEN 'GUMTREE # 16'
                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'NORTH DAVIDSON 46' THEN 'NORTH DAVIDSON # 46'
                WHEN county_nam = 'DAVIDSON' THEN REGEXP_REPLACE(enr_desc, '(\D+\d*)\s+#?(\d+[A-Z]?)$', '\1 #\2')

                WHEN county_nam = 'GASTON' THEN REGEXP_REPLACE(enr_desc, '\s+#?(\d+[A-Z]?)$', ' #\1')
                WHEN county_nam = 'GATES' THEN REGEXP_REPLACE(enr_desc, '\s+#?(\d+)', ' #\1')
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00ANTI$' THEN 'ANTIOCH'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00BERE$' THEN 'BEREA'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00BTNR$' THEN 'BUTNER'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00CORI$' THEN 'CORINTH'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00CRDL$' THEN 'CREDLE'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00CRDM$' THEN 'CREEDMOOR'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00EAOX$' THEN 'EAST OXFORD'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00MTEN$' THEN 'MT ENERGY'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00OKHL$' THEN 'OAK HILL'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00SALM$' THEN 'SALEM'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00SASS$' THEN 'SASSAFRAS FORK'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00SOOX$' THEN 'SOUTH OXFORD'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00TYHO$' THEN 'TALLY HO'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00WILT$' THEN 'WILTON'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00WOEL$' THEN 'WEST OXFORD ELEM.'
                WHEN county_nam = 'GREENE' AND enr_desc = 'Voting District 000SH1' THEN 'SNOW'
                -- WHEN county_nam = 'GREENE' THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))

                WHEN county_nam = 'HENDERSON' THEN REGEXP_REPLACE(enr_desc, '^CV_', '')
                WHEN county_nam = 'HOKE' THEN REGEXP_REPLACE(enr_desc, '\s+#?(\d+)$', ' #\1')
                
                WHEN county_nam = 'IREDELL' THEN REGEXP_REPLACE(enr_desc, '\s+(\d+-?[A-Z]?)$', ' #\1')

                WHEN county_nam = 'JACKSON' AND enr_desc = 'SSW_SND' THEN 'SYLVA DILLSBORO COMBINED'

                WHEN county_nam = 'LEE' THEN concat('PRECINCT ', enr_desc)
                
                WHEN county_nam = 'MADISON' AND enr_desc = 'EBBS C_HAPEL' THEN 'EBBS CHAPEL'
                WHEN county_nam = 'MADISON' AND enr_desc = 'GRAPEV_INE' THEN 'GRAPEVINE'
                WHEN county_nam = 'MADISON' AND enr_desc = 'HOT SP_RINGS' THEN 'HOT SPRINGS'
                WHEN county_nam = 'MADISON' AND enr_desc = 'MARS H_ILL' THEN 'MARS HILL'
                WHEN county_nam = 'MADISON' AND enr_desc = 'RICE COVE' THEN 'REVERE-RICE COVE'
                WHEN county_nam = 'MADISON' THEN replace(enr_desc,'_',' ')
                WHEN county_nam = 'MARTIN' AND enr_desc = 'GOOSENEST' THEN 'GOOSE NEST'
                WHEN county_nam = 'MCDOWELL' THEN REGEXP_REPLACE(enr_desc, '\s+(\d+)$', ' #\1')
                WHEN county_nam = 'MCDOWELL' THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                WHEN county_nam = 'MECKLENBURG' AND enr_desc like '%.%' THEN concat('PCT ', split_part(enr_desc,'.',1))
                WHEN county_nam = 'MECKLENBURG' THEN concat('PCT ', enr_desc)

                WHEN county_nam = 'NORTHAMPTON' AND enr_desc = 'GARYSBURG/PLEASA_PLEASANT HILL' THEN 'GARYSBURG/PLEASANT HILL'
                WHEN county_nam = 'NORTHAMPTON' AND enr_desc = 'LAKE GASTON' THEN 'LAKE GASTON'
                WHEN countY_nam = 'NORTHAMPTON' THEN upper(trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')))
                
                WHEN county_nam = 'ORANGE' AND enr_desc = 'HillsboroughEast' THEN 'HILLSBOROUGH EAST'

                WHEN county_nam = 'PASQUOTANK' AND split_part(enr_desc,'_',1) IN ('NORTH','SOUTH','EAST','WEST') THEN replace(enr_desc,'_',' ')
                WHEN county_nam = 'PERQUIMANS' THEN replace(enr_desc,'_','')
                WHEN county_nam = 'PERSON' THEN upper(split_part(enr_desc,'_',2))
                WHEN county_nam = 'PERSON' THEN upper(enr_desc)
                WHEN county_nam = 'PITT' AND enr_desc = 'GREENVILE  13A' THEN 'GREENVILLE #13A'

                WHEN county_nam = 'ROBESON' AND enr_desc = 'LUMBERTON  1A' THEN 'LUMBERTON #11A'

                WHEN county_nam = 'SCOTLAND' THEN left(enr_desc,2)
                WHEN county_nam = 'STANLY' AND enr_desc = 'North_Albemarle 2' THEN 'NORTH ALBEMARLE 2'
                WHEN county_nam = 'STANLY' THEN concat('PRECINCT ', enr_desc)

                WHEN county_nam = 'YANCEY' THEN REPLACE(REGEXP_REPLACE(enr_desc, '^[^ ]+ ', '', 'g'),'_', '')

                WHEN county_nam = 'WAKE' THEN concat('PRECINCT ', enr_desc)

                WHEN county_nam IN ('HALIFAX','PAMLICO') THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                WHEN county_nam IN ('PITT','RICHMOND','ROBESON','ROWAN','ROCKINGHAM','RUTHERFORD','SURRY','TRANSYLVANIA','WILKES') THEN REGEXP_REPLACE(enr_desc, '(\D+?)\s+(\d+[A-Z]?)$', '\1 #\2')

                WHEN county_nam IN ('ALAMANCE','ANSON','CLEVELAND','CRAVEN','CUMBERLAND','PERSON','STOKES') THEN upper(trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')))
                WHEN county_nam = 'CUMBERLAND' 
                    THEN replace(
                        TRIM(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+_', '', 'g'), 
                                    '_', ' '), 
                                '#', '', 'g')
                            ),
                        '- ','-')
                ELSE trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                
            END AS fixed_precinct_name

        FROM analysis.nc_2020
    ),
    _polling_places_2020 AS (
        SELECT 
            county_name,
            CASE 
                WHEN county_name = 'GREENE' THEN left(precinct_name,4)
                ELSE precinct_name
            END AS short_precinct_name,
            precinct_name,
            polling_place_id,
            polling_place_name
        FROM analysis.nc_polling_places_2020
    )

    SELECT 
        geom,
		g20prertru,
		g20predbid,
		g20preljor,
		g20preghaw,
		g20precbla,
		g20preowri
		g20_total_votes,
        g20prertru - g20predbid AS g20prertru_margin_vote,
        g20predbid - g20prertru AS g20predbid_margin_vote,
        (g20prertru/g20_total_votes) - (g20predbid/g20_total_votes) AS g20prertru_margin_pct,
        (g20predbid/g20_total_votes) - (g20prertru/g20_total_votes) AS g20predbid_margin_pct,
        nc.county_nam,
        nc.enr_desc,
        -- nc.pct_id,
        nc.prec_id,
        nc.clean_enr_desc,
        -- nc.no_space_enr_desc,
        CASE WHEN nc.fixed_precinct_name = '' THEN NULL ELSE nc.fixed_precinct_name END AS fixed_precinct_name, -- ✅ Includes **ALL** fixes
        pp.county_name,
        pp.precinct_name,
        pp.polling_place_id,
        pp.polling_place_name
    FROM _nc_2020 AS nc
    FULL OUTER JOIN _polling_places_2020 AS pp
    ON nc.county_nam = pp.county_name
    AND (
            nc.enr_desc = pp.precinct_name
            OR nc.fixed_precinct_name = pp.precinct_name
            OR nc.fixed_precinct_name = pp.short_precinct_name
            -- OR nc.clean_enr_desc = pp.precinct_name
            -- OR nc.no_space_enr_desc = pp.precinct_name
            OR LOWER(nc.enr_desc) = LOWER(pp.precinct_name)
            OR nc.pct_id = pp.precinct_name
            OR nc.prec_id = pp.precinct_name
            OR nc.prec_id = pp.short_precinct_name
    )
    -- WHERE (nc.county_nam IS NULL OR pp.county_name IS NULL)
    -- WHERE (nc.county_nam = 'ROCKINGHAM' OR pp.county_name = 'MCDOWELL') AND (nc.county_nam IS NULL OR pp.county_name IS NULL)
    -- WHERE (nc.county_nam = 'NORTHAMPTON' OR pp. county_name = 'NORTHAMPTON')
    -- WHERE enr_desc LIKE '%/%' OR precinct_name LIKE '%/%'
    -- WHERE (nc.county_nam = 'ROBESON' OR pp.county_name = 'ROBESON') AND (enr_desc LIKE '%LUMBERTON%' OR precinct_name LIKE '%LUMBERTON%')
    ORDER BY nc.county_nam, nc.enr_desc, pp.county_name, pp.precinct_name
    -- ORDER BY pp.polling_place_id
);


----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_pct_shape_results_polling_locs_voter_reg_2020;
CREATE TABLE analysis.nc_pct_shape_results_polling_locs_voter_reg_2020 AS (
	WITH voter_reg AS (
		SELECT
			county_desc, 
			precinct_abbrv, 
			sum(total_voters) AS total_voters, 
			SUM(total_voters) FILTER (WHERE party_cd = 'DEM') AS total_dem_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'REP') AS total_gop_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'LIB') AS total_lib_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'UNA') AS total_una_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'GRE') AS total_grn_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'CST') AS total_const_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'NLB') AS total_nlb_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'A') AS total_asian_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'B') AS total_black_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'I') AS total_iaan_voters, 
			SUM(total_voters) FILTER (WHERE race_code = 'M') AS total_tomr_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'O') AS total_other_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'P') AS total_nhpi_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'U') AS total_undr_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'W') AS total_white_voters,
			SUM(total_voters) FILTER (WHERE ethnic_code = 'HL') AS total_hl_voters,
			SUM(total_voters) FILTER (WHERE ethnic_code = 'NL') AS total_nhl_voters,
			SUM(total_voters) FILTER (WHERE ethnic_code = 'UN') AS total_undh_voters,
			SUM(total_voters) FILTER (WHERE sex_code = 'F') AS total_fem_voters,
			SUM(total_voters) FILTER (WHERE sex_code = 'M') AS total_male_voters,
			SUM(total_voters) FILTER (WHERE sex_code = 'U') AS total_undg_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age 18 - 25') AS total_18_25_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age 26 - 40') AS total_26_40_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age 41 - 65') AS total_41_65_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age Over 66') AS total_65plus_voters
		FROM analysis.nc_voter_stats_2020
		WHERE precinct_abbrv <> ' '
		GROUP BY county_desc, precinct_abbrv
		-- ORDER BY county_desc, precinct_abbrv
	)
	
	SELECT 
		psrpl.*,
		vr.*,
        CAST(total_dem_voters AS FLOAT) / NULLIF(total_voters, 0) AS dem_pct,
        CAST(total_gop_voters AS FLOAT) / NULLIF(total_voters, 0) AS gop_pct,
        CAST(total_lib_voters AS FLOAT) / NULLIF(total_voters, 0) AS lib_pct,
        CAST(total_una_voters AS FLOAT) / NULLIF(total_voters, 0) AS una_pct,
        CAST(total_grn_voters AS FLOAT) / NULLIF(total_voters, 0) AS grn_pct,
        CAST(total_const_voters AS FLOAT) / NULLIF(total_voters, 0) AS const_pct,
        CAST(total_nlb_voters AS FLOAT) / NULLIF(total_voters, 0) AS nlb_pct,
        CAST(total_asian_voters AS FLOAT) / NULLIF(total_voters, 0) AS asian_pct,
        CAST(total_black_voters AS FLOAT) / NULLIF(total_voters, 0) AS black_pct,
        CAST(total_iaan_voters AS FLOAT) / NULLIF(total_voters, 0) AS iian_pct, 
        CAST(total_tomr_voters AS FLOAT) / NULLIF(total_voters, 0) AS tomr_pct,
        CAST(total_other_voters AS FLOAT) / NULLIF(total_voters, 0) AS other_pct,
        CAST(total_nhpi_voters AS FLOAT) / NULLIF(total_voters, 0) AS nhpi_pct,
        CAST(total_undr_voters AS FLOAT) / NULLIF(total_voters, 0) AS undr_pct,
        CAST(total_white_voters AS FLOAT) / NULLIF(total_voters, 0) AS white_pct,
        CAST(total_hl_voters AS FLOAT) / NULLIF(total_voters, 0) AS hl_pct,
        CAST(total_nhl_voters AS FLOAT) / NULLIF(total_voters, 0) AS nhl_pct,
        CAST(total_undh_voters AS FLOAT) / NULLIF(total_voters, 0) AS undh_pct,
        CAST(total_fem_voters AS FLOAT) / NULLIF(total_voters, 0) AS fem_pct,
        CAST(total_male_voters AS FLOAT) / NULLIF(total_voters, 0) AS male_pct,
        CAST(total_undg_voters AS FLOAT) / NULLIF(total_voters, 0) AS undg_pct,
        CAST(total_18_25_voters AS FLOAT) / NULLIF(total_voters, 0) AS "18_25_pct",
        CAST(total_26_40_voters AS FLOAT) / NULLIF(total_voters, 0) AS "26_40_pct",
        CAST(total_41_65_voters AS FLOAT) / NULLIF(total_voters, 0) AS "41_65_pct",
        CAST(total_65plus_voters AS FLOAT) / NULLIF(total_voters, 0) AS "65plus_pct"
	FROM analysis.nc_pct_shape_results_polling_locs_2020 AS psrpl
	FULL OUTER JOIN voter_reg AS vr
	ON county_name = county_desc
		AND (
			enr_desc = precinct_abbrv
			OR prec_id = precinct_abbrv
		)
	ORDER BY county_nam, enr_desc, county_desc, precinct_abbrv
);

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_pct_shape_polling_locs_2024;
CREATE TABLE analysis.nc_pct_shape_polling_locs_2024 AS (
    WITH _nc_2024 AS (
        SELECT 
            geom,
            replace(county_nam, '_', ' ') AS county_nam,
            trim(enr_desc) AS enr_desc,
            prec_id,
            trim(split_part(enr_desc, '_', 1)) AS pct_id,
            trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')) AS clean_enr_desc,
            trim(REPLACE(enr_desc, ' ', '')) AS no_space_enr_desc,

            CASE 
                WHEN county_nam = 'ALEXANDER' THEN replace(enr_desc,' ',' #')			
                WHEN county_nam = 'AVERY' THEN replace(enr_desc,'   ',' # ') 
                
                WHEN county_nam = 'BLADEN' THEN replace(enr_desc,'  ',' #')
                WHEN county_nam = 'BRUNSWICK' AND enr_desc = '04C1_04B1' THEN 'BELVILLE 2'
                WHEN county_nam = 'BUNCOMBE' AND enr_desc LIKE '% HIGH SCHOOL' THEN REPLACE(enr_desc, ' HIGH SCHOOL', '')
                WHEN county_nam = 'BURKE' AND enr_desc LIKE '%1-%' THEN REGEXP_REPLACE(enr_desc, '([ ]+)(\d)', ' #\2')
                
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'AUMAN-G5A-1' THEN 'AUMAN-1-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'AUMAN-G5A-2' THEN 'AUMAN-2-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CLIFFDALE_WEST- 1-CL57' THEN 'CLIFFDALE WEST-1-CL57'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CLIFFDALE_WEST- 2-CL57' THEN 'CLIFFDALE WEST-2-CL57'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK 23-G2C-1' THEN 'CROSS CREEK 23-1-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK G2C-2' THEN 'CROSS CREEK 23-2-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK 28-G5B-1' THEN 'CROSS CREEK 28-1-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'CROSS CREEK 28-G5B-2' THEN 'CROSS CREEK 28-2-G5'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'HOPE MILLS 2-G9B-1' THEN 'HOPE MILLS 2A-G9'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'HOPE MILLS 2-G9B-2' THEN 'HOPE MILLS 2B-G9'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'Long Hill' THEN 'LONGHILL'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'PEARCES MILL 2-G3A-1' THEN 'PEARCES MILL 2A-G3'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'PEARCES MILL 2-G3A-2' THEN 'PEARCES MILL 2B-G3'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'WESTAREA-G2E-1' THEN 'WESTAREA-1-G2'
                WHEN county_nam = 'CUMBERLAND' AND enr_desc = 'WESTAREA-G2E-2' THEN 'WESTAREA-2-G2'

                WHEN county_nam = 'CALDWELL' AND enr_desc = 'GLOBE/JOHNS RIVER/MULBERRY/WILSON CREEK' THEN 'GLOBE/JOHNS RIVER/MULBERRY/WIL'
                WHEN county_nam = 'CARTERET' AND enr_desc = 'Atlantic/Sea Level/Cedar Island' THEN 'ATLANTIC/SEA LEVEL/CEDAR ISLAN'
                WHEN county_nam = 'CARTERET' AND enr_desc = 'Indian Beach/Salter Path/Pine Knoll' THEN 'INDIAN BEACH/SALTER PATH/PINE '
                WHEN county_nam = 'CARTERET' AND enr_desc = 'OTWAY/STRAITS/BETTIE/Gloucester' THEN 'OTWAY/STRAITS/BETTIE/GLOUCESTE'
                WHEN county_nam = 'CASWELL' AND enr_desc = 'YANCEYVILLE 2' THEN 'YANCEYVILLE'
                WHEN county_nam = 'CATAWBA' THEN REGEXP_REPLACE(enr_desc, '([ ]+)(\d)', ' #\2')
                WHEN county_nam = 'CRAVEN' AND enr_desc = 'DOVER-FORT BARNWELL West' THEN 'DOVER-FORT BARNWELL'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'BETHWR' THEN 'BETHWARE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'FALSTN' THEN 'FALLSTON'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KINGST' THEN 'KINGSTOWN'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KM N' THEN 'KINGS MOUNTAIN NORTH'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'KM S' THEN 'KINGS MOUNTAIN SOUTH'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'LATT' THEN 'LATTIMORE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'LAWNDL' THEN 'LAWNDALE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'MRB-YO' THEN 'MOORESBORO-YOUNG'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'OAKGRV' THEN 'OAK GROVE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'POLKVL' THEN 'POLKVILLE'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'SHANGI' THEN 'SHANGHAI'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'Shelby 4' THEN 'SHELBY #4'
                WHEN county_nam = 'CLEVELAND' AND enr_desc = 'S 5' THEN 'SHELBY #5'

                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'COTTON GROVE 10' THEN 'COTTON GROVE  #10'
                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'EMMONS 14' THEN 'EMMONS # 14'
                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'GUMTREE 16' THEN 'GUMTREE # 16'
                WHEN county_nam = 'DAVIDSON' AND enr_desc = 'NORTH DAVIDSON 46' THEN 'NORTH DAVIDSON # 46'
                WHEN county_nam = 'DAVIDSON' THEN REGEXP_REPLACE(enr_desc, '(\D+\d*)\s+#?(\d+[A-Z]?)$', '\1 #\2')

                WHEN county_nam = 'GASTON' THEN REGEXP_REPLACE(enr_desc, '\s+#?(\d+[A-Z]?)$', ' #\1')
                WHEN county_nam = 'GATES' THEN REGEXP_REPLACE(enr_desc, '\s+#?(\d+)', ' #\1')
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00ANTI$' THEN 'ANTIOCH'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00BERE$' THEN 'BEREA'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00BTNR$' THEN 'BUTNER'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00CORI$' THEN 'CORINTH'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00CRDL$' THEN 'CREDLE'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00CRDM$' THEN 'CREEDMOOR'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00EAOX$' THEN 'EAST OXFORD'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00MTEN$' THEN 'MT ENERGY'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00OKHL$' THEN 'OAK HILL'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00SALM$' THEN 'SALEM'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00SASS$' THEN 'SASSAFRAS FORK'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00SOOX$' THEN 'SOUTH OXFORD'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00TYHO$' THEN 'TALLY HO'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00WILT$' THEN 'WILTON'
                WHEN county_nam = 'GRANVILLE' AND enr_desc ~ '00WOEL$' THEN 'WEST OXFORD ELEM.'
                WHEN county_nam = 'GREENE' AND enr_desc = 'Voting District 000SH1' THEN 'SNOW'
                -- WHEN county_nam = 'GREENE' THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))

                WHEN county_nam = 'HARNETT' AND prec_id = 'PR25' THEN 'EAST AVERASBORO'
                WHEN county_nam = 'HENDERSON' THEN REGEXP_REPLACE(enr_desc, '^CV_', '')
                WHEN county_nam = 'HOKE' THEN REGEXP_REPLACE(enr_desc, '\s+#?(\d+)$', ' #\1')
                
                WHEN county_nam = 'IREDELL' THEN REGEXP_REPLACE(enr_desc, '\s+(\d+)-?\s?([A-Z]?)$', ' #\1\2')

                WHEN county_nam = 'JACKSON' AND enr_desc = 'SSW_SND' THEN 'SYLVA DILLSBORO COMBINED'
                WHEN county_nam = 'JOHNSTON' AND prec_id = 'PR25' THEN 'WEST SELMA'

                WHEN county_nam = 'LEE' THEN concat('PRECINCT ', enr_desc)
                
                WHEN county_nam = 'MADISON' AND enr_desc = 'EBBS C_HAPEL' THEN 'EBBS CHAPEL'
                WHEN county_nam = 'MADISON' AND enr_desc = 'GRAPEV_INE' THEN 'GRAPEVINE'
                WHEN county_nam = 'MADISON' AND enr_desc = 'HOT SP_RINGS' THEN 'HOT SPRINGS'
                WHEN county_nam = 'MADISON' AND enr_desc = 'MARS H_ILL' THEN 'MARS HILL'
                WHEN county_nam = 'MADISON' AND enr_desc = 'RICE COVE' THEN 'REVERE-RICE COVE'
                WHEN county_nam = 'MADISON' THEN replace(enr_desc,'_',' ')
                WHEN county_nam = 'MARTIN' AND enr_desc = 'GOOSENEST' THEN 'GOOSE NEST'
                WHEN county_nam = 'MCDOWELL' THEN REGEXP_REPLACE(enr_desc, '\s+(\d+)$', ' #\1')
                WHEN county_nam = 'MCDOWELL' THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                WHEN county_nam = 'MECKLENBURG' AND enr_desc like '%.%' THEN concat('PCT ', split_part(enr_desc,'.',1))
                WHEN county_nam = 'MECKLENBURG' THEN concat('PCT ', enr_desc)

                WHEN county_nam = 'NORTHAMPTON' AND enr_desc = 'GARYSBURG/PLEASA_PLEASANT HILL' THEN 'GARYSBURG/PLEASANT HILL'
                WHEN county_nam = 'NORTHAMPTON' AND enr_desc = 'LAKE GASTON' THEN 'LAKE GASTON'
                WHEN countY_nam = 'NORTHAMPTON' THEN upper(trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')))
                
                WHEN county_nam = 'ORANGE' AND enr_desc = 'HillsboroughEast' THEN 'HILLSBOROUGH EAST'

                WHEN county_nam = 'PASQUOTANK' AND split_part(enr_desc,'_',1) IN ('NORTH','SOUTH','EAST','WEST') THEN replace(enr_desc,'_',' ')
                WHEN county_nam = 'PERQUIMANS' THEN replace(enr_desc,'_','')
                WHEN county_nam = 'PERSON' THEN upper(split_part(enr_desc,'_',2))
                WHEN county_nam = 'PERSON' THEN upper(enr_desc)
                WHEN county_nam = 'PITT' AND enr_desc = 'GREENVILE  13A' THEN 'GREENVILLE #13A'

                WHEN county_nam = 'ROBESON' AND enr_desc = 'LUMBERTON  1A' THEN 'LUMBERTON #11A'

                WHEN county_nam = 'SCOTLAND' THEN left(enr_desc,2)
                WHEN county_nam = 'STANLY' AND enr_desc = 'North_Albemarle 2' THEN 'NORTH ALBEMARLE 2'
                WHEN county_nam = 'STANLY' THEN concat('PRECINCT ', enr_desc)

                WHEN county_nam = 'YANCEY' THEN REPLACE(REGEXP_REPLACE(enr_desc, '^[^ ]+ ', '', 'g'),'_', '')

                WHEN county_nam = 'WAKE' THEN concat('PRECINCT ', enr_desc)

                WHEN county_nam IN ('HALIFAX','PAMLICO') THEN trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                WHEN county_nam IN ('PITT','RICHMOND','ROBESON','ROWAN','ROCKINGHAM','RUTHERFORD','SURRY','TRANSYLVANIA','WILKES') THEN REGEXP_REPLACE(enr_desc, '(\D+?)\s+(\d+[A-Z]?)$', '\1 #\2')

                WHEN county_nam IN ('ALAMANCE','ANSON','CLEVELAND','CRAVEN','CUMBERLAND','PERSON','STOKES') THEN upper(trim(REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+ ', '', 'g')))
                WHEN county_nam = 'CUMBERLAND' 
                    THEN replace(
                        TRIM(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(enr_desc, '^[A-Z0-9-]+_', '', 'g'), 
                                    '_', ' '), 
                                '#', '', 'g')
                            ),
                        '- ','-')
                ELSE trim(REGEXP_REPLACE(split_part(enr_desc, '_', 2), '[\n\r]+', '', 'g'))
                
            END AS fixed_precinct_name

        FROM analysis.sbe_precincts_20240723
    ),
    _polling_places_2024 AS (
        SELECT 
            county_name,
            CASE 
                WHEN county_name = 'GREENE' THEN left(precinct_name,4)
                WHEN county_name = 'UNION' THEN right(precinct_name,3)
                ELSE precinct_name
            END AS short_precinct_name,
            precinct_name,
            polling_place_id,
            polling_place_name
        FROM analysis.nc_polling_places_2024
    )

    SELECT 
        geom,
        coalesce(nc.county_nam,pp.county_name) AS county_nam,
        nc.enr_desc,
        -- nc.pct_id,
        nc.prec_id,
        -- nc.clean_enr_desc,
        -- nc.no_space_enr_desc,
        CASE WHEN nc.fixed_precinct_name = '' THEN NULL ELSE nc.fixed_precinct_name END AS fixed_precinct_name, -- ✅ Includes **ALL** fixes
        pp.county_name,
        pp.precinct_name,
        pp.polling_place_id,
        pp.polling_place_name
    FROM _nc_2024 AS nc
    FULL OUTER JOIN _polling_places_2024 AS pp
    ON nc.county_nam = pp.county_name
    AND (
            nc.enr_desc = pp.precinct_name
            OR nc.fixed_precinct_name = pp.precinct_name
            OR nc.fixed_precinct_name = pp.short_precinct_name
            -- OR nc.clean_enr_desc = pp.precinct_name
            -- OR nc.no_space_enr_desc = pp.precinct_name
            OR LOWER(nc.enr_desc) = LOWER(pp.precinct_name)
            OR nc.pct_id = pp.precinct_name
            OR nc.prec_id = pp.precinct_name
            OR nc.prec_id = pp.short_precinct_name
    )
    -- WHERE (nc.county_nam IS NULL OR pp.county_name IS NULL)
    -- WHERE (nc.county_nam = 'ROCKINGHAM' OR pp.county_name = 'MCDOWELL') AND (nc.county_nam IS NULL OR pp.county_name IS NULL)
    -- WHERE (nc.county_nam = 'FORSYTH' OR pp. county_name = 'FORSYTH')
    -- WHERE enr_desc LIKE '%/%' OR precinct_name LIKE '%/%'
    -- WHERE (nc.county_nam = 'ROBESON' OR pp.county_name = 'ROBESON') AND (enr_desc LIKE '%LUMBERTON%' OR precinct_name LIKE '%LUMBERTON%')
	-- WHERE precinct_name like '%-07%' AND county_name = 'WAKE'
    ORDER BY nc.county_nam, nc.enr_desc, pp.county_name, pp.precinct_name
    -- ORDER BY pp.polling_place_id
);

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_pct_results_2024;
CREATE TABLE analysis.nc_pct_results_2024 AS (
    SELECT 
        county,
        precinct_name AS rslts_precinct_name,
        precinct_code AS rslts_precinct_code,
		CASE 
			WHEN precinct_name is null AND LEFT(precinct_code,3) = 'EV-' THEN split_part(precinct_code,'-',2)
			WHEN precinct_name is null AND LEFT(precinct_code,3) = 'EV ' THEN split_part(precinct_code,' ',2)
			WHEN precinct_name is null AND RIGHT(precinct_code,3) = ' EV' THEN split_part(precinct_code,' ',1)
			WHEN precinct_name is null AND precinct_code like 'EARLY VOTING %' THEN split_part(precinct_code,'EARLY VOTING ',2)
			ELSE precinct_name
		END AS normalized_precinct,
        -- Total votes in the precinct
        SUM(vote_ct) AS total_votes,

        -- Sum of votes per candidate
        SUM(CASE WHEN candidate_name = 'Kamala D. Harris' THEN vote_ct ELSE 0 END) AS harris_votes,
        SUM(CASE WHEN candidate_name = 'Donald J. Trump' THEN vote_ct ELSE 0 END) AS trump_votes,
        SUM(CASE WHEN candidate_name = 'Chase Oliver' THEN vote_ct ELSE 0 END) AS oliver_votes,
        SUM(CASE WHEN candidate_name = 'Jill Stein' THEN vote_ct ELSE 0 END) AS stein_votes,
        SUM(CASE WHEN candidate_name = 'Cornel West' THEN vote_ct ELSE 0 END) AS west_votes,
        SUM(CASE WHEN candidate_name = 'Claudia De la Cruz' THEN vote_ct ELSE 0 END) AS dela_cruz_votes,
        SUM(CASE WHEN candidate_name = 'Randall Terry' THEN vote_ct ELSE 0 END) AS terry_votes,
        SUM(CASE WHEN candidate_name = 'Shiva Ayyadurai' THEN vote_ct ELSE 0 END) AS ayyadurai_votes,
        SUM(CASE WHEN candidate_name = 'Write-In (Miscellaneous)' THEN vote_ct ELSE 0 END) AS write_in_votes,
        SUM(CASE WHEN candidate_name = 'OVER VOTE' THEN vote_ct ELSE 0 END) AS over_votes,
        SUM(CASE WHEN candidate_name = 'UNDER VOTE' THEN vote_ct ELSE 0 END) AS under_votes,

        -- Sum of votes per voting method
        SUM(CASE WHEN voting_method_lbl = 'V' THEN vote_ct ELSE 0 END) AS election_day_votes,
        SUM(CASE WHEN voting_method_lbl = 'EV' THEN vote_ct ELSE 0 END) AS early_voting_votes,
        SUM(CASE WHEN voting_method_lbl = 'M' THEN vote_ct ELSE 0 END) AS absentee_votes,
        SUM(CASE WHEN voting_method_lbl = 'P' THEN vote_ct ELSE 0 END) AS provisional_votes,
        SUM(CASE WHEN voting_method_lbl = 'T' THEN vote_ct ELSE 0 END) AS transfer_votes,

        -- Candidate percentages
        cast(SUM(CASE WHEN candidate_name = 'Kamala D. Harris' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS harris_pct,
        cast(SUM(CASE WHEN candidate_name = 'Donald J. Trump' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS trump_pct,
        cast(SUM(CASE WHEN candidate_name = 'Chase Oliver' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS oliver_pct,
        cast(SUM(CASE WHEN candidate_name = 'Jill Stein' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS stein_pct,
        cast(SUM(CASE WHEN candidate_name = 'Cornel West' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS west_pct,
        cast(SUM(CASE WHEN candidate_name = 'Claudia De la Cruz' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS dela_cruz_pct,
        cast(SUM(CASE WHEN candidate_name = 'Randall Terry' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS terry_pct,
        cast(SUM(CASE WHEN candidate_name = 'Shiva Ayyadurai' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS ayyadurai_pct,
        cast(SUM(CASE WHEN candidate_name = 'Write-In (Miscellaneous)' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS write_in_pct,

        -- Harris-Trump Margin
        cast((SUM(CASE WHEN candidate_name = 'Kamala D. Harris' THEN vote_ct ELSE 0 END) - 
        SUM(CASE WHEN candidate_name = 'Donald J. Trump' THEN vote_ct ELSE 0 END)) AS FLOAT) AS harris_trump_margin,

        -- Voting method percentages
        cast(SUM(CASE WHEN voting_method_lbl = 'V' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS election_day_pct,
        cast(SUM(CASE WHEN voting_method_lbl = 'EV' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS early_voting_pct,
        cast(SUM(CASE WHEN voting_method_lbl = 'M' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS absentee_pct,
        cast(SUM(CASE WHEN voting_method_lbl = 'P' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS provisional_pct,
        cast(SUM(CASE WHEN voting_method_lbl = 'T' THEN vote_ct ELSE 0 END) / NULLIF(SUM(vote_ct), 0) AS FLOAT) AS transfer_pct

    FROM analysis.statewide_precinct_sort_20241105
    WHERE contest_title = 'US PRESIDENT'
    AND precinct_name IS NOT NULL
    GROUP BY county, precinct_name, precinct_code
);

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_pct_shape_results_polling_locs_2024;
CREATE TABLE analysis.nc_pct_shape_results_polling_locs_2024 AS (
	SELECT 
		nc.*,
		pp.*
	FROM analysis.nc_pct_shape_polling_locs_2024 AS nc
	FULL OUTER JOIN analysis.nc_pct_results_2024 AS pp
	ON county_nam = county
	    AND (
			nc.enr_desc = TRIM(pp.rslts_precinct_name)
			OR nc.fixed_precinct_name = TRIM(pp.rslts_precinct_name)
			OR LOWER(nc.enr_desc) = TRIM(LOWER(pp.rslts_precinct_name))
			OR nc.prec_id = TRIM(pp.rslts_precinct_name)
			OR nc.precinct_name = TRIM(pp.rslts_precinct_name)
	    )
	-- WHERE (county_nam is null OR county is null)
	-- WHERE (county_nam = 'ALAMANCE' OR county = 'ALAMANCE')
	-- WHERE ((county_nam is null AND enr_desc is not null) OR county = 'WAKE')
	WHERE (pp.rslts_precinct_name is not null OR pp.normalized_precinct is not null)
	ORDER BY county_nam, enr_desc, county, pp.rslts_precinct_name
);

----------------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_pct_shape_results_polling_locs_voter_reg_2024;
CREATE TABLE analysis.nc_pct_shape_results_polling_locs_voter_reg_2024 AS (
	WITH voter_reg AS (
		SELECT
			county_desc, 
			precinct_abbrv, 
			sum(total_voters) AS total_voters, 
			SUM(total_voters) FILTER (WHERE party_cd = 'DEM') AS total_dem_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'REP') AS total_gop_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'LIB') AS total_lib_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'UNA') AS total_una_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'GRE') AS total_grn_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'CST') AS total_const_voters,
			SUM(total_voters) FILTER (WHERE party_cd = 'NLB') AS total_nlb_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'A') AS total_asian_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'B') AS total_black_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'I') AS total_iaan_voters, 
			SUM(total_voters) FILTER (WHERE race_code = 'M') AS total_tomr_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'O') AS total_other_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'P') AS total_nhpi_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'U') AS total_undr_voters,
			SUM(total_voters) FILTER (WHERE race_code = 'W') AS total_white_voters,
			SUM(total_voters) FILTER (WHERE ethnic_code = 'HL') AS total_hl_voters,
			SUM(total_voters) FILTER (WHERE ethnic_code = 'NL') AS total_nhl_voters,
			SUM(total_voters) FILTER (WHERE ethnic_code = 'UN') AS total_undh_voters,
			SUM(total_voters) FILTER (WHERE sex_code = 'F') AS total_fem_voters,
			SUM(total_voters) FILTER (WHERE sex_code = 'M') AS total_male_voters,
			SUM(total_voters) FILTER (WHERE sex_code = 'U') AS total_undg_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age 18 - 25') AS total_18_25_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age 26 - 40') AS total_26_40_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age 41 - 65') AS total_41_65_voters,
			SUM(total_voters) FILTER (WHERE age = 'Age Over 66') AS total_65plus_voters
		FROM analysis.nc_voter_stats_2024
		WHERE precinct_abbrv <> ' '
		GROUP BY county_desc, precinct_abbrv
		-- ORDER BY county_desc, precinct_abbrv
	)
	
	SELECT 
        psrpl.*,
		vr.*,
        CAST(total_dem_voters AS FLOAT) / NULLIF(total_voters, 0) AS dem_pct,
        CAST(total_gop_voters AS FLOAT) / NULLIF(total_voters, 0) AS gop_pct,
        CAST(total_lib_voters AS FLOAT) / NULLIF(total_voters, 0) AS lib_pct,
        CAST(total_una_voters AS FLOAT) / NULLIF(total_voters, 0) AS una_pct,
        CAST(total_grn_voters AS FLOAT) / NULLIF(total_voters, 0) AS grn_pct,
        CAST(total_const_voters AS FLOAT) / NULLIF(total_voters, 0) AS const_pct,
        CAST(total_nlb_voters AS FLOAT) / NULLIF(total_voters, 0) AS nlb_pct,
        CAST(total_asian_voters AS FLOAT) / NULLIF(total_voters, 0) AS asian_pct,
        CAST(total_black_voters AS FLOAT) / NULLIF(total_voters, 0) AS black_pct,
        CAST(total_iaan_voters AS FLOAT) / NULLIF(total_voters, 0) AS iian_pct, 
        CAST(total_tomr_voters AS FLOAT) / NULLIF(total_voters, 0) AS tomr_pct,
        CAST(total_other_voters AS FLOAT) / NULLIF(total_voters, 0) AS other_pct,
        CAST(total_nhpi_voters AS FLOAT) / NULLIF(total_voters, 0) AS nhpi_pct,
        CAST(total_undr_voters AS FLOAT) / NULLIF(total_voters, 0) AS undr_pct,
        CAST(total_white_voters AS FLOAT) / NULLIF(total_voters, 0) AS white_pct,
        CAST(total_hl_voters AS FLOAT) / NULLIF(total_voters, 0) AS hl_pct,
        CAST(total_nhl_voters AS FLOAT) / NULLIF(total_voters, 0) AS nhl_pct,
        CAST(total_undh_voters AS FLOAT) / NULLIF(total_voters, 0) AS undh_pct,
        CAST(total_fem_voters AS FLOAT) / NULLIF(total_voters, 0) AS fem_pct,
        CAST(total_male_voters AS FLOAT) / NULLIF(total_voters, 0) AS male_pct,
        CAST(total_undg_voters AS FLOAT) / NULLIF(total_voters, 0) AS undg_pct,
        CAST(total_18_25_voters AS FLOAT) / NULLIF(total_voters, 0) AS "18_25_pct",
        CAST(total_26_40_voters AS FLOAT) / NULLIF(total_voters, 0) AS "26_40_pct",
        CAST(total_41_65_voters AS FLOAT) / NULLIF(total_voters, 0) AS "41_65_pct",
        CAST(total_65plus_voters AS FLOAT) / NULLIF(total_voters, 0) AS "65plus_pct"
	FROM analysis.nc_pct_shape_results_polling_locs_2024 AS psrpl
	FULL OUTER JOIN voter_reg AS vr
	ON county_name = county_desc
		AND (
			enr_desc = precinct_abbrv
			OR prec_id = precinct_abbrv
		)
	ORDER BY county_nam, enr_desc, county_desc, precinct_abbrv
);