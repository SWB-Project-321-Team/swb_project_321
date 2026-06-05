# Analysis Plan Working Draft

## Scope

The analysis plan working draft is meant to outline all of the analysis that will be included in the final report. Note that the final report will also include documentation related to data provenance, which is not outlined here.

## Format

Each section of the analysis plan should include the following, as specifically as possible:

- What questions will it attempt to answer?
- What data elements will be needed?
- What data sources will they be derived from and exactly what variables will be used?
- What visualizations will be produced?
- What tables will be produced?
- What statistical tests will be performed? What assumptions will be tested in order to justify the use of the test?
- What types of conclusions do we expect to draw?

Note on consistency: To whatever extent is reasonable, we should be consistent in our choice of statistical tests and visualization methods across various sections, when the same conditions apply, rather than each picking our favorite.

## Collaboration Approach

Each section has an identified point person who will take the lead on drafting content. All team members are encouraged to leave comments, questions, and suggestions as the draft evolves, using the comment function. Topics that get too complex for this method should be discussed directly via Slack or in a meeting.

## Analysis Plan

## Section 1. Demographic Comparison Of Target And Benchmark Regions

This section builds upon the analysis that was performed during benchmark region selection. It compares characteristics such as population size, industry makeup, income statistics, and other characteristics that do not relate directly to charitable donations or nonprofit organizations.

**Basic question:** How do the demographic and economic characteristics of the Black Hills region compare with Benchmark Regions?

**Reference:** The benchmark region analysis that was developed during the benchmark region selection process.

### 1. How does the population size of the Black Hills region compare with benchmark regions?

- Data elements needed: Population totals
- Available data source: American Community Survey (ACS)
- Visualization: Bar chart comparing population across regions
- Table: Population by region
- Expected conclusion: Whether the Black Hills population base is comparable to benchmark regions
- Expected insight: Whether nonprofit comparisons should be adjusted for population, and whether nonprofit counts should be normalized

### 2. How large are the benchmark regions geographically compared to the Black Hills region?

- Data elements needed: Geographic area (square miles)
- Available data source: Derived from benchmark definition
- Visualization: Map visualization or bar chart
- Expected conclusion: Contextualize nonprofit density comparisons

### 3. How does household income compare across regions?

- Data elements needed: Median household income; mean earnings
- Available data source: American Community Survey (ACS)
- Visualization: Bar chart comparisons
- Table: Income comparison table
- Expected conclusion: Whether income levels are lower or higher, affecting capacity for charitable giving

### 4. What is the labour market strength in the Black Hills compared to benchmark regions?

- Data elements needed: Unemployment rate; population 16+ in labor force; civilian employed population
- Available data source: American Community Survey (ACS)
- Visualization: Clustered bar chart and bar chart
- Table: Labor force statistics table
- Expected conclusion: Assess economic stability affecting nonprofit demand

### 5. How does occupational structure differ between regions?

- Data elements needed: Occupation distribution (Management/Business/Science; Service; Sales/Office; Natural Resources/Construction)
- Available data source: American Community Survey (ACS)
- Visualization: Stacked bar chart showing occupational distribution
- Table: Occupation distribution table
- Statistical test: Chi-square test for occupational distribution differences
- Expected conclusion: Identify differences in economic base that may affect philanthropy

### 6. How do household structures differ across regions?

- Data elements needed: Household counts and earning households (total households, households with earnings, households with retirement income)
- Available data source: American Community Survey (ACS)
- Visualization: Clustered bar chart for household counts and earning households
- Table: Household characteristics table
- Expected conclusion: Determine income-earning capacity and donor base; identify potential presence of older donor populations

### 7. How does household income distribution differ across regions?

- Data elements needed: Income tiers: low (<$50k), middle ($50k-$149k), high ($150k+)
- Available data source: American Community Survey (ACS)
- Visualization: Stacked bar chart
- Table: Income distribution table
- Statistical test: Chi-square test of distribution differences
- Expected conclusion: Determine income structure per region

## Section 2. Donor Characterization

This section describes tax-reported donor behavior using IRS SOI data, focusing on how charitable giving varies across regions and conditional on income levels.

Note: IRS Statistics of Income (SOI) county-level individual income tax data are currently available only through tax year 2022. This dataset captures only one year (2022) within the intended study period (2022-2024).

### 1. How do donor participation rates compare across regions within income groups?

- Variables: Total returns; returns with charitable contributions (count); total adjusted gross income (AGI) group; county FIPS code
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022)
- Analysis method: Participation rate = returns with charitable contributions (count) / total returns; stratify by AGI group; compare participation rate across regions; descriptive comparison using mean and median participation rate per region and AGI group
- Presentation: Boxplot of participation rate by region within income group; table of mean and median participation rate by region
- Expected conclusions: How donor participation rates in Black Hills compare descriptively to benchmark regions within the same income group, and whether any observed differences are consistent across income groups or concentrated in specific AGI groups

### 2. How does the average charitable contribution per donor compare across regions within income groups?

- Variables: Total charitable contributions; returns with charitable contributions; AGI group; county FIPS code
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022)
- Analysis method: Average contribution per donor = total charitable contributions / returns with charitable contributions; stratify by AGI group; log transform if distribution is skewed; descriptive comparison using mean and median average gift per region and AGI group
- Presentation: Boxplot of average contribution by region; line plot of income group versus average contribution by region; summary statistics table
- Expected conclusions: How average gift size in Black Hills compares descriptively to benchmark regions within the same income group, after accounting for income level; identifies whether any observed gap is consistent across income groups or concentrated at a particular group

### 3. How do charitable contributions relative to income compare across regions?

- Variables: Total charitable contributions; total adjusted gross income (AGI); AGI group; county FIPS code
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022)
- Analysis method: Giving ratio = total charitable contributions / total adjusted gross income (AGI); stratify by AGI group; ZIP to county FIPS; compare distributions across regions; descriptive comparison using mean and median giving ratio per region and AGI group
- Presentation: Boxplot of giving ratio by region; heatmap of region x income group
- Expected conclusions: How the giving ratio in Black Hills compares descriptively to benchmark regions within the same income group

### 4. Are regional differences in total giving driven by participation or contribution size?

- Variables: Total returns; returns with charitable contributions; total charitable contributions; AGI group; county FIPS code
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022)
- Analysis method: Participation rate = returns with charitable contributions (count) / total returns; average contribution per donor = total charitable contributions / returns with charitable contributions; compare contribution drivers across regions; assess whether differences arise from participation versus contribution size
- Presentation: Decomposition bar chart (participation versus average contribution); side-by-side comparison plots
- Expected conclusions: Whether the Black Hills giving profile is driven primarily by a broader donor base (participation) or by higher per-donor amounts (intensity)

### 5. Does the relationship between income and charitable giving differ across regions?

- Variables: Total charitable contributions; returns with charitable contributions; total AGI; AGI group
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022)
- Analysis method: Participation rate = returns with charitable contributions (count) / total returns; average contribution per donor = total charitable contributions / returns with charitable contributions; giving ratio = total charitable contributions / total adjusted gross income (AGI); evaluate trends across income groups; compare slopes across regions at region level
- Presentation: Line plot of income group versus giving metrics by region; comparative trend visualization
- Expected conclusions: How the income-giving gradient in Black Hills compares visually to benchmark regions, and whether higher-income donors appear to drive giving disproportionately in certain regions

### 6. Is rurality associated with donor participation rates across counties?

- Variables: Donor participation rate by AGI group; Rural-Urban Continuum Code (RUCC); county FIPS code
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022); USDA Rural-Urban Continuum Codes
- Analysis method: Spearman rank correlation between RUCC code and participation rate pooled across all counties in the study regions; simple linear regression with RUCC as predictor and participation rate as outcome, one model per AGI group; analysis unit is individual counties
- Presentation: Scatter plot of RUCC code versus participation rate by AGI group; correlation coefficient table
- Expected conclusions: Whether counties with higher rurality (higher RUCC codes) show systematically lower donor participation rates, and whether this relationship holds consistently across AGI groups

### 7. Is nonprofit density associated with donor participation rates across counties?

- Variables: Donor participation rate by AGI group; number of nonprofit organizations per county; population per county; county FIPS code
- Dataset: IRS SOI CBSA/County Data (Tax Year 2022); IRS EO-BMF; ACS
- Analysis method: Spearman rank correlation between nonprofit density and participation rate; simple linear regression with nonprofit density as predictor, one model per AGI group; nonprofit density = number of nonprofits / population x 10,000; analysis unit is individual counties
- Presentation: Scatter plot of nonprofit density versus participation rate; correlation coefficient table
- Expected conclusions: Whether counties with higher nonprofit density show higher donor participation rates, suggesting that supply-side infrastructure (availability of recipient organizations) is associated with giving behavior

### Notes

- SOI data reflect itemized tax returns only
- No statistical significance testing (Q1-Q3, Q5): Billings, Missoula, and Flagstaff each consist of a single county, making distributional tests. Conclusions describe observed patterns and magnitudes, not statistical significance.
- Results represent tax-deductible giving behavior, not total population giving

## Section 3. Non-Profit Organization Characterization

This section describes nonprofit organizations including size, budget, funding sources, and mission types, with comparison between the target and benchmark regions.

### 1. Is there a significant difference in the number and density of nonprofit organizations between Black Hills and benchmark regions?

- Variables: Total number of nonprofit organizations for each region, identified by EIN; population size
- Datasets: IRS EO-BMF; American Community Survey
- Analysis method: Density = number of nonprofits / 10K population; ANOVA
- Presentation: ANOVA result; bar graphs of number and density of nonprofits by region

### 2. Is there a rural and/or indigenous service gaps within the Black Hills community (compare counties)?

- Variables: County (address); total number of organizations for each county; Rural-Urban Continuum Codes; indigenous population
- Datasets: IRS EO-BMF; American Community Survey; Economic Research Service
- Analysis method: Not specified in source draft
- Presentation: Map of nonprofit density per county

### 3. Is there a significant difference in total revenue between Black Hills and benchmark regions?

- Variables: Total revenue; NTEE filed classification code
- Datasets: IRS EO-BMF
- Analysis method: Normalize revenue by number of nonprofits for each region; ANOVA; with and without hospitals, universities, and political organizations
- Presentation: ANOVA result; bar graph of normalized total revenue for each region

### 4. Is there a significant difference in total assets between Black Hills and benchmark regions?

- Variables: Total assets; NTEE filed classification code
- Datasets: IRS EO-BMF
- Analysis method: Normalize assets by number of nonprofits for each region; ANOVA; with and without hospitals, universities, and political organizations
- Presentation: ANOVA result and any conclusions about statistical significance; bar graph of normalized total assets for each region

### 5. Is there a significant difference in nonprofits' dependency on grants vs. other revenue sources between Black Hills and benchmark regions?

- Variables: Grants (total amount); total revenue; NTEE field classification code
- Datasets: USAspending.gov; Form 990; IRS EO-BMF
- Analysis method: Percentage of grants in total revenue; ANOVA; with and without hospitals, universities, and political organizations
- Presentation: ANOVA result; bar graph of percentage of grants in total revenue for each region

### 6. Are there any nonprofit fields under/over-represented in each region?

- Variables: NTEE field classification code, using only the broad category first letter; total revenue; total asset
- Datasets: IRS EO-BMF
- Analysis method: Chi-squared test
- Presentation: Chi-squared test result; heatmap of region versus NTEE fields; total revenue and asset by field (bar, pie)

### 7. What are the best predictors of total revenue for Black Hills and each benchmark region?

- Variables: Total revenue; percent of armed forces; average annual employment; average annual wages; percent of broad NTEE field classification code
- Datasets: IRS EO-BMF; American Community Survey; Bureau of Labor Statistics
- Analysis method: Normalize revenue by the number of nonprofits for each region as the response variable; multiple regression
- Presentation: Multiple regression result in a table comparing the significance of predictors for each region

### 8. How has the number of nonprofit organizations and performance changed over time (2022-2024)?

- Variables: Number of nonprofit organizations for each region for each year; population
- Datasets: IRS EO-BMF
- Analysis method: Multiple regression for each variable controlling for population
- Presentation: Regression result in a table; line graph normalized for change in population size

## For Limited Number Of Organizations (Form 990, EZ, PF)

### 1. Is there a difference in the revenue sources between Black Hills and benchmark regions?

- Variables: Total revenue; program service revenue; total contributions; other contributions (foundation grants etc.)
- Datasets: NCCS
- Analysis method: Total contributions for 990, EZ, and PF; program service revenue from 990 and EZ; ANOVA
- Presentation: Stacked bar graph; pie chart for Black Hills region (communication only)

### 2. Is there a difference in financial performance of nonprofit organizations between Black Hills and benchmark regions (2022-2024)?

- Variables: Total revenue; total expense; net asset
- Datasets: Form 990
- Analysis method: Months of reserves = (net assets / total expenses) x 12; net margin = (surplus = revenue - expenses) / total revenue; multiple regression controlling for population
- Presentation: Regression result in a table; line graph

## Notes

### 1. Form 990 vs. IRS EO-BMF

- IRS EO-BMF includes all IRS-recognized organizations that are tax-exempt, including 501c3 organizations, private foundations, and political organizations
- Organizations that are not required to file Form 990 are included, such as churches
- Only total revenue, income, and assets are available
- Form 990 is filed by organizations with gross receipts over $200K or total assets over $500K
- Form 990 shows detailed revenue source and expense data for each organization
- Form 990-EZ is filed by organizations with gross receipts $50K to $200K
- Form 990-PF is filed by private foundations regardless of financial status
- Form 990-N is filed by organizations with less than $50K gross receipts
- Form 990-N provides no financial details

### 2. NTEE codes

- Reference: <https://urbaninstitute.github.io/nccs-legacy/ntee/ntee.html>
- Hospitals:
  - E20 - Hospitals
  - E21 - Community Health Systems
  - E22 - General Hospitals
  - E24 - Specialty Hospitals
- Universities:
  - B40 - Higher Education
  - B41 - Two-Year Colleges
  - B42 - Undergraduate Colleges
  - B43 - Universities
  - B50 - Graduate and Professional Schools

### 3. Political organizations

- 501c4 - social welfare, but also often political according to Eric
- 527 - political committees
