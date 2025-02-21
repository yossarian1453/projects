/*
The sources vary from third party to the State of NC:
https://election.lab.ufl.edu/data-archive/north-carolina/
https://github.com/PublicI/us-polling-places/blob/update-2020/data/north_carolina/output/North%20Carolina_2012-11-06.csv
https://www2.census.gov/acs2012_1yr/summaryfile/2012_ACSSF_By_State_All_Tables/
https://dl.ncsbe.gov/?prefix=
https://dl.ncsbe.gov/?prefix=PrecinctMaps/
https://www.ncsbe.gov/results-data/polling-place-data
https://www.ncsbe.gov/results-data/election-results/historical-election-results-data#by-precinct
https://www.ncsbe.gov/results-data/voter-registration-data
https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/layout_voter_stats.txt
https://dl.ncsbe.gov/index.html?prefix=data/Snapshots/
https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/layout_results_pct.txt
https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/layout_results_precinct_sort.txt
https://s3.amazonaws.com/dl.ncsbe.gov/ENRS/layout_results_pct.txt
https://www.ncsbe.gov/results-data/voter-turnout
https://www2.census.gov/geo/tiger/TIGER2020PL/STATE/37_NORTH_CAROLINA/
*/



DROP TABLE analysis.nc_polling_places_2012;
CREATE TABLE analysis.nc_polling_places_2012 (
    election_date DATE,
    state VARCHAR,
    county_name VARCHAR,
    jurisdiction VARCHAR,
    jurisdiction_type VARCHAR,
    precinct_id VARCHAR,
    precinct_name VARCHAR,
    polling_place_id INT,
    location_type VARCHAR,
    name VARCHAR,
    address VARCHAR,
    notes VARCHAR,
    source VARCHAR,
    source_date DATE,
    source_notes VARCHAR
);


COPY analysis.nc_polling_places_2012
FROM '/Users/robertness/projects/polling_locations/data/nc/nc_polling_place_20121106.csv'
WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER ',');

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_precinct_results_2012
CREATE TABLE analysis.nc_precinct_results_2012 (
    county TEXT,
    precinct TEXT,
    contest_type TEXT,
    runoff_status TEXT,
    recount_status TEXT,
    contest TEXT,
    choice TEXT,
    winner_status TEXT,
    party TEXT,
    election_day INT,
    one_stop INT,
    absentee_by_mail INT,
    provisional INT,
    total_votes INT,
    district TEXT
);

COPY analysis.nc_precinct_results_2012
FROM '/Users/robertness/projects/polling_locations/data/nc/results_pct_20121106.txt'
WITH (FORMAT CSV, HEADER);


--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_polling_places_2016;
CREATE TABLE analysis.nc_polling_places_2016 (
    election_date DATE,
    county_name VARCHAR,
    polling_place_id INT,
    polling_place_name VARCHAR,
    precinct_name VARCHAR,
    house_num VARCHAR,
    street_name VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR
);

COPY analysis.nc_polling_places_2016
FROM '/Users/robertness/projects/polling_locations/data/nc/nc_polling_place_20161108_utf8.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_polling_places_2020;
CREATE TABLE analysis.nc_polling_places_2020 (
    election_date DATE,
    county_name VARCHAR,
    polling_place_id INT,
    polling_place_name VARCHAR,
    precinct_name VARCHAR,
    house_num VARCHAR,
    street_name VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR
);

COPY analysis.nc_polling_places_2020
FROM '/Users/robertness/projects/polling_locations/data/nc/nc_polling_place_20201103_utf8.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.nc_polling_places_2024;
CREATE TABLE analysis.nc_polling_places_2024 (
    election_dt DATE,
    county_name VARCHAR,
    polling_place_id INT,
    polling_place_name VARCHAR,
    precinct_name VARCHAR,
    street_address VARCHAR,
    city VARCHAR,
    state VARCHAR,
    zip VARCHAR
);

COPY analysis.nc_polling_places_2024
FROM '/Users/robertness/projects/polling_locations/data/nc/polling_place_20241105_utf8.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.results_pct_20241105;
CREATE TABLE analysis.results_pct_20241105 (
	row_num int,
    county VARCHAR,
    election_date DATE,
    precinct VARCHAR,
    contest_group_id VARCHAR,
    contest_type VARCHAR,
    contest_name VARCHAR,
    choice VARCHAR,
    choice_party VARCHAR,
    vote_for INT,
    election_day INT,
    early_voting INT,
    absentee_by_mail INT,
    provisional INT,
    total_votes INT,
    real_precinct VARCHAR
);

COPY analysis.results_pct_20241105
FROM '/Users/robertness/projects/polling_locations/data/nc/results_pct_20241105_utf8.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.statewide_precinct_sort_20241105;
CREATE TABLE analysis.statewide_precinct_sort_20241105 (
    county_id VARCHAR,
    county VARCHAR,
    election_dt DATE,
    result_type_lbl VARCHAR,
    result_type_desc VARCHAR,
    contest_id VARCHAR,
    contest_title VARCHAR,
    contest_party_lbl VARCHAR,
    contest_vote_for VARCHAR,
    precinct_code VARCHAR,
    precinct_name VARCHAR,
    candidate_id VARCHAR,
    candidate_name VARCHAR,
    candidate_party_lbl VARCHAR,
    group_num VARCHAR,
    group_name VARCHAR,
    voting_method_lbl VARCHAR,
    voting_method_rslt_desc VARCHAR,
    vote_ct INT
);

COPY analysis.statewide_precinct_sort_20241105
FROM '/Users/robertness/projects/polling_locations/data/nc/statewide_precinct_sort_20241105_utf8.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

DROP TABLE analysis.statewide_precinct_sort_20241105;
CREATE TABLE analysis.statewide_precinct_sort_20241105 (
    county_id VARCHAR,
    county VARCHAR,
    election_dt DATE,
    result_type_lbl VARCHAR,
    result_type_desc VARCHAR,
    contest_id VARCHAR,
    contest_title VARCHAR,
    contest_party_lbl VARCHAR,
    contest_vote_for VARCHAR,
    precinct_code VARCHAR,
    precinct_name VARCHAR,
    candidate_id VARCHAR,
    candidate_name VARCHAR,
    candidate_party_lbl VARCHAR,
    group_num VARCHAR,
    group_name VARCHAR,
    voting_method_lbl VARCHAR,
    voting_method_rslt_desc VARCHAR,
    vote_ct INT
);

COPY analysis.statewide_precinct_sort_20241105
FROM '/Users/robertness/projects/polling_locations/data/nc/statewide_precinct_sort_20241105_utf8.csv'
WITH (FORMAT CSV, HEADER);

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

CREATE TABLE analysis.nc_voter_registration (
    snapshot_dt DATE,
    county_id INT,
    county_desc VARCHAR,
    voter_reg_num VARCHAR,
    ncid VARCHAR,
    status_cd VARCHAR,
    voter_status_desc VARCHAR,
    reason_cd VARCHAR,
    voter_status_reason_desc VARCHAR,
    absent_ind VARCHAR,
	name_prefx_cd VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    midl_name VARCHAR,
    name_sufx_cd VARCHAR,
    house_num VARCHAR,
    half_code VARCHAR,
    street_dir VARCHAR,
    street_name VARCHAR,
    street_type_cd VARCHAR,
    street_sufx_cd VARCHAR,
    unit_designator VARCHAR,
    unit_num VARCHAR,
    res_city_desc VARCHAR,
    state_cd VARCHAR,
    zip_code VARCHAR,
    mail_addr1 VARCHAR,
    mail_addr2 VARCHAR,
    mail_addr3 VARCHAR,
    mail_addr4 VARCHAR,
    mail_city VARCHAR,
    mail_state VARCHAR,
    mail_zipcode VARCHAR,
    area_cd VARCHAR,
    phone_num VARCHAR,
    race_code VARCHAR,
    race_desc VARCHAR,
    ethnic_code VARCHAR,
    ethnic_desc VARCHAR,
    party_cd VARCHAR,
    party_desc VARCHAR,
    sex_code VARCHAR,
    sex VARCHAR,
    age INT,
    birth_place VARCHAR,
    registr_dt DATE,
    precinct_abbrv VARCHAR,
    precinct_desc VARCHAR,
    municipality_abbrv VARCHAR,
    municipality_desc VARCHAR,
    ward_abbrv VARCHAR,
    ward_desc VARCHAR,
    cong_dist_abbrv VARCHAR,
    cong_dist_desc VARCHAR,
    super_court_abbrv VARCHAR,
    super_court_desc VARCHAR,
    judic_dist_abbrv VARCHAR,
    judic_dist_desc VARCHAR,
    nc_senate_abbrv VARCHAR,
    nc_senate_desc VARCHAR,
    nc_house_abbrv VARCHAR,
    nc_house_desc VARCHAR,
    county_commiss_abbrv VARCHAR,
    county_commiss_desc VARCHAR,
    township_abbrv VARCHAR,
    township_desc VARCHAR,
    school_dist_abbrv VARCHAR,
    school_dist_desc VARCHAR,
    fire_dist_abbrv VARCHAR,
    fire_dist_desc VARCHAR,
    water_dist_abbrv VARCHAR,
    water_dist_desc VARCHAR,
    sewer_dist_abbrv VARCHAR,
    sewer_dist_desc VARCHAR,
    sanit_dist_abbrv VARCHAR,
    sanit_dist_desc VARCHAR,
    rescue_dist_abbrv VARCHAR,
    rescue_dist_desc VARCHAR,
    munic_dist_abbrv VARCHAR,
    munic_dist_desc VARCHAR,
    dist_1_abbrv VARCHAR,
    dist_1_desc VARCHAR,
    dist_2_abbrv VARCHAR,
    dist_2_desc VARCHAR,
    confidential_ind VARCHAR,
    cancellation_dt DATE,
    vtd_abbrv VARCHAR,
    vtd_desc VARCHAR,
    load_dt DATE,
    age_group VARCHAR
);
COPY analysis.nc_voter_registration
FROM '/Users/robertness/projects/polling_locations/data/nc/VR_Snapshot_20161108_clean.txt'
WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER E'\t');

--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

CREATE TABLE analysis.nc_voter_registration_2020 (
    snapshot_dt DATE,
    county_id INT,
    county_desc VARCHAR,
    voter_reg_num VARCHAR,
    ncid VARCHAR,
    status_cd VARCHAR,
    voter_status_desc VARCHAR,
    reason_cd VARCHAR,
    voter_status_reason_desc VARCHAR,
    absent_ind VARCHAR,
    name_prefx_cd VARCHAR,
    last_name VARCHAR,
    first_name VARCHAR,
    midl_name VARCHAR,
    name_sufx_cd VARCHAR,
    house_num VARCHAR,
    half_code VARCHAR,
    street_dir VARCHAR,
    street_name VARCHAR,
    street_type_cd VARCHAR,
    street_sufx_cd VARCHAR,
    unit_designator VARCHAR,
    unit_num VARCHAR,
    res_city_desc VARCHAR,
    state_cd VARCHAR,
    zip_code VARCHAR,
    mail_addr1 VARCHAR,
    mail_addr2 VARCHAR,
    mail_addr3 VARCHAR,
    mail_addr4 VARCHAR,
    mail_city VARCHAR,
    mail_state VARCHAR,
    mail_zipcode VARCHAR,
    area_cd VARCHAR,
    phone_num VARCHAR,
    race_code VARCHAR,
    race_desc VARCHAR,
    ethnic_code VARCHAR,
    ethnic_desc VARCHAR,
    party_cd VARCHAR,
    party_desc VARCHAR,
    sex_code VARCHAR,
    sex VARCHAR,
    age INT,
    birth_place VARCHAR,
    registr_dt DATE,
    precinct_abbrv VARCHAR,
    precinct_desc VARCHAR,
    municipality_abbrv VARCHAR,
    municipality_desc VARCHAR,
    ward_abbrv VARCHAR,
    ward_desc VARCHAR,
    cong_dist_abbrv VARCHAR,
    cong_dist_desc VARCHAR,
    super_court_abbrv VARCHAR,
    super_court_desc VARCHAR,
    judic_dist_abbrv VARCHAR,
    judic_dist_desc VARCHAR,
    nc_senate_abbrv VARCHAR,
    nc_senate_desc VARCHAR,
    nc_house_abbrv VARCHAR,
    nc_house_desc VARCHAR,
    county_commiss_abbrv VARCHAR,
    county_commiss_desc VARCHAR,
    township_abbrv VARCHAR,
    township_desc VARCHAR,
    school_dist_abbrv VARCHAR,
    school_dist_desc VARCHAR,
    fire_dist_abbrv VARCHAR,
    fire_dist_desc VARCHAR,
    water_dist_abbrv VARCHAR,
    water_dist_desc VARCHAR,
    sewer_dist_abbrv VARCHAR,
    sewer_dist_desc VARCHAR,
    sanit_dist_abbrv VARCHAR,
    sanit_dist_desc VARCHAR,
    rescue_dist_abbrv VARCHAR,
    rescue_dist_desc VARCHAR,
    munic_dist_abbrv VARCHAR,
    munic_dist_desc VARCHAR,
    dist_1_abbrv VARCHAR,
    dist_1_desc VARCHAR,
    dist_2_abbrv VARCHAR,
    dist_2_desc VARCHAR,
    confidential_ind VARCHAR,
    cancellation_dt DATE,
    vtd_abbrv VARCHAR,
    vtd_desc VARCHAR,
    load_dt DATE,
    age_group VARCHAR
);
COPY analysis.nc_voter_registration_2020
FROM '/Users/robertness/projects/polling_locations/data/nc/VR_Snapshot_20201103_clean.txt'
WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER E'\t');


--------------------------------------------------------------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------

CREATE TABLE analysis.nc_voter_stats_2016 (
    county_desc VARCHAR,
    election_date DATE,
    stats_type VARCHAR,
    precinct_abbrv VARCHAR,
    vtd_abbrv VARCHAR,
    party_cd VARCHAR,
    race_code VARCHAR,
    ethnic_code VARCHAR,
    sex_code VARCHAR,
    age INT,
    total_voters INT,
    update_date DATE
);
COPY analysis.nc_voter_stats_2016
FROM '/Users/robertness/projects/polling_locations/data/nc/voter_stats_20161108.txt'
WITH (FORMAT CSV, HEADER, QUOTE '"', DELIMITER E'\t');
