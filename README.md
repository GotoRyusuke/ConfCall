# ConfCall

## Introduction
This is a project to:
- :heavy_check_mark: calculte word & sent freqs for confcall transcripts;
- :heavy_check_mark: drop transcropts for confcalls where there is no speech record for CEO or CFO, and 
aggregate to panel data;
- :heavy_check_mark: based on the results in panel data, sum up indicators for each manager(CEO or CFO) and get the 
cross-sectional data

## Structure of the project
### 1.cal word&sent freqs
Tow modules named `cal_sf` and `cal_wf` to calculate word & sent freqs for transcripts we have. The `cal_sf` module utilises another module called `moral_sent_classifier` to do sent classification.

After the raw panel data is done, use `raw2panel` to drop records where there is no speech from CEO or CFO, and get the final panel data.

### 2. cross data
The module `Panel2Cross` in this folder is for *compressing* panel data to cross-sectional data, by adding up indicators for each individual CFO or CEO.
