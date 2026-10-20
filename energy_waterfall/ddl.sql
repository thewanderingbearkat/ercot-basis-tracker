-- GOLD.PERFORMANCE: business-ready performance facts derived from BRONZE.BAZEFIELD + BRONZE.VAISALA_POWERUP.
-- Built by ercot-basis-tracker energy_waterfall (refresh.py). Window rebuilds are DELETE + INSERT by plant and HE_UTC.
-- Runs as SYSADMIN in the interim, per the medallion layout (one database per layer).

CREATE DATABASE IF NOT EXISTS GOLD COMMENT = 'Medallion gold layer: business-ready facts, marts and apps';

CREATE SCHEMA IF NOT EXISTS GOLD.PERFORMANCE COMMENT = 'Asset performance: energy waterfall facts and app';

CREATE TABLE IF NOT EXISTS GOLD.PERFORMANCE.ENERGY_WATERFALL_HOURLY (
    PLANT_CODE      VARCHAR(8)     NOT NULL COMMENT 'Fleet DIM plant key, e.g. LFW01',
    HE_UTC          TIMESTAMP_NTZ  NOT NULL COMMENT 'Hour ending, UTC',
    LOCAL_DATE      DATE           NOT NULL COMMENT 'Plant-local calendar date of the hour (hour start, local time)',
    OBJECT_KEY      VARCHAR(32)    NOT NULL COMMENT 'Turbine key (LFW01-WTG001) or SITE for site-level rows',
    STEP_ORDER      NUMBER(2)      NOT NULL COMMENT '0 P50 potential, 1 resource, 2 potential at actual wind, 3-8 losses, 9 turbine gross',
    STEP            VARCHAR(64)    NOT NULL,
    SUBCATEGORY     VARCHAR(128)   NOT NULL COMMENT 'Ribbon sub-category, e.g. GADS category or SCADA class',
    DETAIL          VARCHAR(256)   NOT NULL COMMENT 'Root cause (Bazefield allocation) or detail, may be empty',
    COMPENSATION    VARCHAR(64)             COMMENT 'Curtailment steps only: presumed compensated (node RT LMP at or below IPL threshold) or not',
    MWH             FLOAT          NOT NULL COMMENT 'Steps 3-8 and 1: energy lost (positive = loss). Steps 0, 2, 9: energy',
    TURBINE_HOURS   FLOAT                   COMMENT 'Turbine-hours in this bucket',
    BUILD_ID        VARCHAR(16)    NOT NULL
) COMMENT = 'Energy waterfall per turbine-hour: P50 potential - resource = potential at actual wind - losses = turbine gross';

CREATE TABLE IF NOT EXISTS GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY (
    PLANT_CODE          VARCHAR(8)     NOT NULL,
    HE_UTC              TIMESTAMP_NTZ  NOT NULL,
    LOCAL_DATE          DATE           NOT NULL,
    LT_POTENTIAL_MWH    FLOAT COMMENT 'P50 potential: k x PowerUp 782 mean of 2015-2025 for the same hour of year',
    BUDGET_GROSS_MWH    FLOAT COMMENT 'Monthly budget gross generation shaped to hours by the long-term profile',
    PU_HUB_WIND_MS      FLOAT COMMENT 'PowerUp element 26 hub-height wind, actual hour',
    PU_HUB_WIND_LT_MS   FLOAT COMMENT 'PowerUp element 26, 2015-2025 mean for the same hour of year',
    BZ_ACTUAL_MWH       FLOAT COMMENT 'Bazefield site ActivePowerWTG',
    BZ_THEORETICAL_MWH  FLOAT COMMENT 'Bazefield site TheoreticalPower (reference curve, ISO density)',
    BZ_UNAVAILABLE_MWH  FLOAT COMMENT 'Bazefield GADS lost production in PBA categories (Available = false), prorated to hours',
    BZ_LOST_ALL_MWH     FLOAT COMMENT 'Bazefield GADS lost production, all categories, prorated to hours',
    TURBINES_MEASURED   FLOAT COMMENT 'Average turbines with SCADA data in the hour',
    TURBINES_TOTAL      NUMBER(4),
    RT_LMP              FLOAT COMMENT 'MISO real-time final LMP, ALTW.LKFLD.IPL',
    DA_LMP              FLOAT COMMENT 'MISO day-ahead ex-post LMP, ALTW.LKFLD.IPL',
    PRICE_SOURCE        VARCHAR(32) COMMENT 'Price used for the compensation test',
    BUILD_ID            VARCHAR(16)    NOT NULL
) COMMENT = 'Energy waterfall site context: long-term, budget, wind and Bazefield tie-out per hour';

CREATE TABLE IF NOT EXISTS GOLD.PERFORMANCE.ENERGY_WATERFALL_BUILD (
    BUILD_ID                   VARCHAR(16)   NOT NULL,
    PLANT_CODE                 VARCHAR(8)    NOT NULL,
    WINDOW_START_UTC           TIMESTAMP_NTZ,
    WINDOW_END_UTC             TIMESTAMP_NTZ,
    BUILT_AT                   TIMESTAMP_NTZ,
    POWERUP_K                  FLOAT COMMENT 'Measured potential / PowerUp modelled output on well-measured hours',
    STOPPED_ANEMOMETER_FACTOR  FLOAT COMMENT 'Stopped-turbine nacelle wind / running-turbine site median',
    LT_YEARS                   VARCHAR(16),
    METHOD                     VARCHAR(256),
    FACT_ROWS                  NUMBER
) COMMENT = 'One row per waterfall build (method parameters for audit)';

CREATE TABLE IF NOT EXISTS GOLD.PERFORMANCE.BUDGET_GENERATION_MONTHLY (
    PLANT_CODE        VARCHAR(8)  NOT NULL,
    MONTH             DATE        NOT NULL,
    BUDGET_GROSS_MWH  FLOAT       NOT NULL,
    SOURCE            VARCHAR(256),
    LOADED_AT         TIMESTAMP_NTZ
) COMMENT = 'Budget gross generation by plant-month (FP&A monthly master file), seeded from config/budget_generation_monthly.csv'
;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_HOURLY ADD COLUMN IF NOT EXISTS COMPENSATION VARCHAR(64);

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS RT_LMP FLOAT;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS DA_LMP FLOAT;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS PRICE_SOURCE VARCHAR(32)
;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS RESOURCE_ACTUAL FLOAT;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS RESOURCE_LT FLOAT;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS RESOURCE_UNIT VARCHAR(32);

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS METER_MWH FLOAT;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS METER_SOURCE VARCHAR(64);

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS BZ_METER_MWH FLOAT;

ALTER TABLE GOLD.PERFORMANCE.ENERGY_WATERFALL_SITE_HOURLY ADD COLUMN IF NOT EXISTS RELATED_METER_MWH FLOAT
