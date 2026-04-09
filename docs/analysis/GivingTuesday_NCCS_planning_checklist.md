# GivingTuesday And NCCS Planning Checklist

## Purpose
This checklist translates the team meeting discussion into a concrete planning-phase task list for the GivingTuesday and NCCS nonprofit data.

This is **not** the full data-processing phase yet.

The immediate goal is to decide:

- what the cleaned data should look like
- what variables the analysis team actually needs
- how the GivingTuesday and NCCS sources can be harmonized to support that analysis
- what questions need clarification before full processing begins

## Data In Scope
For this planning phase, treat the 990-related sources as one related family of datasets:

- GivingTuesday 990 data
- NCCS efile
- NCCS Core
- NCCS postcard
- NCCS BMF

If Publication 4573 is relevant to understanding missing or jointly filed organizations, treat it as a related supporting source, but not the main harmonization target.

## Main Working Assumption
Unless the analysis plan explicitly says otherwise, assume the team wants:

- one **harmonized organization-year dataset**
- organizations analyzed together regardless of filing type
- variables mapped across forms where possible
- form-specific differences handled through harmonization and documentation

Do **not** assume the analysis should be broken out by filing form unless the analysis plan specifically requests that.

## What You Are Supposed To Do Now

### 1. Read The Analysis Plan And Pull Out The Relevant Questions
Go through the analysis plan draft and identify every section that depends on GivingTuesday or NCCS nonprofit data.

For each relevant section, note:

- the analysis question
- the variables it appears to need
- whether the section implies pooled analysis across organizations
- whether the section explicitly requires subgrouping by some category

### 2. Identify The Needed Variables
Build a working list of variables that the analysis team seems to need from the GivingTuesday and NCCS sources.

Examples likely to matter:

- EIN
- tax year
- organization name
- filing form
- organization type or classification
- revenue
- expenses
- assets
- income or net surplus/deficit
- hospital indicator
- university indicator
- NTEE or subsection information
- geography fields if relevant

### 3. Figure Out How Those Variables Exist Across The Different 990 Sources
For each needed variable, identify:

- which source datasets contain it
- which form types contain it
- the exact field/column/line where it appears
- whether it exists in comparable form across datasets
- whether it needs harmonization across forms

This is the start of the harmonized data dictionary.

### 4. Propose The Shape Of The Cleaned Output
Propose what the cleaned 990-related deliverable should look like.

Default working proposal:

- one harmonized table per tax year
- one row per organization-year
- common columns shared across sources/forms
- provenance or documentation describing where each harmonized field came from

If you think some fields cannot be meaningfully harmonized across all forms, note that explicitly.

### 5. Identify Dataset Idiosyncrasies And Risks
For each source, note the main quirks that affect harmonization or analysis.

Examples:

- variables that only exist on full 990 but not 990-EZ or postcard
- fields that differ in meaning across forms
- hospital/university identification fields
- fields that might require classification rather than exact numeric extraction
- organizations that may be missing because of joint filing or related filing structure

### 6. Clarify Open Questions With The Analysis Team
For small questions:

- leave comments directly in the analysis plan
- tag the section owner
- say exactly how you are interpreting the data requirement and ask them to confirm

For larger unresolved issues:

- bring them to the Tuesday meeting

## Recommended Interpretation From The Meeting

### Hospitals And Universities
The likely priority is:

- identify whether an organization is a hospital
- identify whether an organization is a university
- allow those organizations to be separated analytically if needed

The likely priority is **not**:

- extracting highly detailed hospital-specific revenue substreams unless the team confirms that they want that

### Filing Types
The current meeting interpretation supports:

- harmonizing organizations together where possible
- not building separate analysis structures by filing type unless the analysis plan explicitly requires it

### Time Structure
The likely working format is:

- one harmonized deliverable at the organization-year level

The meeting did **not** suggest that the immediate priority is building a complex longitudinal tracking system for specific organizations across many years.

## Deliverables For This Planning Phase

### Deliverable 1. Variable And Analysis Crosswalk
A short document or table that links:

- analysis section
- analysis question
- required variable
- source dataset(s)
- form(s)
- notes on harmonization difficulty

This is the minimum deliverable for showing that the data plan matches the analysis plan.

### Deliverable 2. Draft Harmonized Data Dictionary
A draft dictionary that, for each harmonized variable, states:

- harmonized variable name
- plain-language definition
- source dataset(s)
- exact source field(s), form(s), or line-item location(s)
- harmonization rule or mapping logic
- important caveats

This does not need to be final by Tuesday, but there should be a usable draft.

### Deliverable 3. Proposed Output Structure
A short proposal describing what the cleaned GivingTuesday/NCCS deliverable should look like.

At minimum, describe:

- unit of analysis
- one row per what
- one file per what
- key columns that must exist
- whether filing types are pooled or separated
- what provenance/documentation should accompany the dataset

### Deliverable 4. Open Questions List
A short list of unresolved questions that need confirmation from the analysis team.

Examples:

- whether any analyses need to be broken out by filing type
- whether detailed hospital revenue fields are actually needed
- whether classification fields are enough for hospital/university treatment
- whether any variables are required that only exist on some forms

### Deliverable 5. Tuesday Meeting Readout
Be ready to explain:

- how you think the cleaned dataset should be organized
- what variables you think can be harmonized cleanly
- what variables are difficult or form-specific
- what assumptions you made from the analysis plan
- what still needs clarification

This can be verbal, but having a written checklist, draft dictionary, or mock schema is preferred if time allows.

## What You Do Not Need To Finish Yet
You do **not** need to complete all of the following before Tuesday:

- full data cleaning
- complete ETL implementation
- exhaustive line-by-line extraction from every form
- final polished documentation
- every possible harmonized variable

The priority is to get enough clarity to confirm that the team is aiming at the right cleaned-data structure before full processing begins.

## Suggested Work Sequence

1. Read the analysis plan and mark every section that uses 990-related data.
2. Pull out the variables each section appears to require.
3. Compare those variables across GivingTuesday and NCCS sources/forms.
4. Start a harmonized data dictionary draft.
5. Write a short proposal for the cleaned output structure.
6. Add small clarification comments to the analysis plan and tag the section leads.
7. Bring bigger issues to the Tuesday meeting.

## Bottom Line
Your job right now is to design the harmonized structure for the GivingTuesday and NCCS nonprofit data, not to finish all processing.

By Tuesday, you should be able to show:

- what the cleaned 990-related dataset should look like
- what variables it should contain
- how those variables map across forms and sources
- what still needs confirmation from the analysis team
