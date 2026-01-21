# Spring 2026: Rice D2K Capstone Project Proposal

## Project Title

Streamflow forecasting along the Mississippi River

## Project Pitch

Streamflow forecasting is a central challenge in hydrology, with applications in flood risk management, water resources allocation, drought mitigation, and ecosystem protection. The problem involves predicting future river discharge given meteorological forcings and basin characteristics. Accurate forecasts are essential for supporting operational decisions such as reservoir management, hydropower generation, irrigation scheduling, and early warning systems for extreme events. As climate variability and extremes intensify, the importance of reliable streamflow predictions continues to grow.

In the United States, flooding (including from river basin overflow) is among the most frequent and deadly natural hazards. According to the U.S. Geological Survey, floods account for over 75% of federally declared natural disasters and cause an average of $8 billion in damages annually, along with over 90 deaths each year. From 1959 to 2019, the contiguous United States recorded approximately 6,478 flood-related fatalities, averaging over 100 deaths annually, with flash floods being the deadliest subtype. In recent years, such as 2024, flood-related deaths rose even higher, with 145 fatalities reported nationwide.

The Mississippi River Basin is one of the largest drainage systems in the U.S., integrating numerous tributaries (e.g., the Ohio, Missouri, Arkansas, Red rivers) and exhibiting a wide range of hydrologic, climatic, and physiographic conditions. The Missouri River Basin is the largest single tributary basin of the Mississippi River system, spanning over 500,000 square miles across ten U.S. states and encompassing a wide range of hydrologic, climatic, and physiographic conditions. The basin includes diverse headwater regions in the Rocky Mountains, semi-arid plains, agricultural watersheds, and heavily managed river reaches. Because of its size, elevation variability, and mixture of regulated and unregulated tributaries, the Missouri Basin presents substantial forecasting challenges including snowmelt-driven hydrology, long flow travel times, pronounced seasonal cycles, and extensive reservoir operations.

Hydrologic analyses in the Missouri Basin may underscore its complexity and sensitivity to climatic and management factors. For instance, natural region weather patterns typically drive two annual runoff peaks, one in late spring from mountain snowmelt and another during summer rainfall. Recent research also indicates that streamflow variability and runoff trends in the basin have increased in the 20th and 21st centuries, driven by a combination of climate variability, land-use change, and altered hydrologic regimes. This variability in flow behavior, combined with large spatial variation in hydrologic response, makes streamflow forecasting in the Missouri Basin both scientifically challenging and societally important. This is especially true in light of water resource demands, flood risk, and agricultural dependence across the region.

## Project Description

We aim to build on approaches common in large-sample hydrology and machine learning using the CAMELS (Catchment Attributes and Meteorology for Large-sample Studies) dataset. CAMELS provides topological attributes (geology, land cover, soil, topography) and observed hourly (or daily) streamflow records for many catchments, making it suitable for large-sample water science and ML applications. It additionally offers meteorological driver data for each gauge. For example, in Chile, CAMELS analogues have been used to characterize hydro-climatic conditions across catchments. By offering both static watershed descriptors and meteorological drivers, CAMELS enables comparative studies across diverse regions and may support generalization to ungauged or data-scarce basins.

## Project Objectives

- Wrangle the public dataset CAMELSH to generate a single dataset along the Mississippi River.
- Visualize the geographic and climate attributes of the gauges within the Mississippi River.
- Implement statistical analysis of the gauges within the Mississippi River to identify the variables related to streamflow/flood forecasting.
- Optimize transformer models for streamflow forecasting and derive feature importance.

## Data Description

The CAMELSH (Catchment Attributes and Hourly HydroMeteorology for Large-Sample Studies) is published in 2025 and freely accessible to everyone.

Specifically, the latest version of CAMELSH includes hourly streamflow and water level data for a total of 5,767 gauges from 1980-2024 over US.

In addition, CAMELSH provides 9 climate attributes including:
- mean daily precipitation (p_mean)
- mean daily potential evaporation (pet_mean)
- aridity index (aridity_index)
- seasonality and timing of precipitation (p_seasonality)
- fraction of precipitation falling as snow (frac_snow)
- frequency of high precipitation days (high_prec_freq)
- average duration of high precipitation events (high_prec_dur)
- frequency of low precipitation days (low_prec_freq)
- average duration of low precipitation events (low_prec_dur)

## Data Confidentiality

**Is your data confidential? Does it have privacy and/or security requirements to be considered? Does data have Protected health information (PHI) that is necessary for the project?**

NO

**By what mechanism will you be sharing the data with Rice?**

Google Drive. Students can freely access the public data by themselves.

## Presentations and Disclosure

This is a course at Rice University and students will need to present their work.

**Public presentations:** Student teams will present their findings at the end of semester D2K Showcase in the form of a 1 minute elevator pitch video as well as a short presentation and poster session. No Confidential Information will be included in the presentations. The 1 minute elevator pitch video will also be posted publicly on the D2K website.

**Do you consent to public presentation of this project?** Yes

**Internal presentations:** In class, student teams will present and discuss their findings with instructors and with other students. No Confidential Information will be included in the presentations.

**Do you consent to the internal presentation and discussion of this project in a class setting?** Yes

**Do you consent to public disclosure (title and one sentence description) of this project on the D2K website and social media?** Yes

## Student Teams

| Category | Details |
|---|---|
| Project Type | Civil engineering/resiliency/public interest |
| Technical Area(s) | Machine learning, time series forecasting |
| Student Preferences | ECE, CS, Civil engineering |
| Required Skills | Data wrangling and visualization, statistical analysis |
| Preferred Skills | Time series forecasting, transformers, spatial-temporal forecasting |
