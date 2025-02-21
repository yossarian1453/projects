# Impact of Precinct Closures on Voter Turnout in North Carolina

## Overview
This project examines the impact of precinct closures on voter turnout in North Carolina during the 2016, 2020, and 2024 elections. Using voter registration data, precinct-level election results, and demographic information, the analysis determines whether polling place reductions disproportionately affected specific voter groups.

## Data Sources and Cleaning
- Data was sourced primarily from the **North Carolina State Board of Elections**, the **University of Florida**, and the **Center for Public Integrity**. The full list of sources is available in `uploads.sql`.
- Matching precincts across datasets was a challenge due to inconsistent naming conventions between sources, including within North Carolina's own data.
- The data pipeline is currently ad hoc, primarily focused on loading data into **PostgreSQL**. While the queries used to compile the data are not yet fully automated, manual cleaning revealed patterns that could make automation possible in the future.

## Goals of the Analysis
- Identify precinct closures by tracking precincts that disappeared between election years.
- Measure turnout changes from 2020 to 2024 at the precinct level.
- Assess the impact of precinct closures on different demographic groups.
- Use machine learning models (**XGBoost, Random Forest**) to estimate turnout loss due to closures.

---

## How Precinct Closures Were Determined
Since precinct closures were not explicitly labeled, they were inferred based on the following criteria:

1. **Tracking the Number of Precincts Over Time**  
   - The number of unique `precinct_name` values was counted for each election year (2016, 2020, 2024).  
   - Precincts that existed in 2016 or 2020 but were missing in 2024 were considered closed.

2. **Comparing Turnout Before and After Closures**  
   - Turnout rates were calculated for each election year using:  
     `Turnout = Total Votes / Registered Voters`
   - The model identified precincts where turnout declined significantly between 2020 and 2024.

3. **Correlating Demographics with Turnout Loss**  
   - The analysis examined demographic factors (race, party affiliation, age, gender) to determine if turnout losses disproportionately affected minority-heavy or Democratic-leaning precincts.

4. **County vs. State-Level Analysis**  
   - Precinct-level trends showed direct impacts of closures.  
   - County-wide voter registration growth was analyzed to differentiate local effects from broader statewide trends.  
   - Statewide precinct counts confirmed an overall reduction in the number of precincts.

---

## Key Findings
- **Precinct closures disproportionately affected Black, Democratic, and younger voters.**
- **Republican, White, and older precincts were better at maintaining turnout.**
- **Polling place reductions had a measurable structural impact on voter participation.**
- **Voter registration growth helped mitigate some losses, but not entirely.**

---

## Methodology
### **1. Data Processing**
- Standardized column names across 2016, 2020, and 2024 datasets.
- Merged precinct-level voter registration and election data.
- Computed turnout rates and voter registration growth at both the precinct and county levels.

### **2. Model Training (XGBoost & Random Forest)**
- **Target variable:** Turnout change from 2020 to 2024.
- **Predictors included:**
  - Demographics (race, gender, age groups, party registration)
  - Precinct-level and county-level voter registration growth
- **Hyperparameter tuning:** GridSearchCV optimized **Random Forest** parameters for best performance.

### **3. Statistical & Machine Learning Analysis**
- **Feature importance analysis** identified the top predictors of turnout loss.
- **Correlation analysis** confirmed that precinct closures were linked to racial and partisan disparities.


