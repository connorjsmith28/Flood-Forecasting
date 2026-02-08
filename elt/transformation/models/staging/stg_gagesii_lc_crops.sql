-- Core GAGES-II landscape crop attributes
select
    staid as site_id,
    CDL_CORN as cdl_corn,
    CDL_COTTON as cdl_cotton,
    CDL_RICE as cdl_rice,
    CDL_SORGHUM as cdl_sorghum,
    CDL_SOYBEANS as cdl_soybeans,
    CDL_SUNFLOWERS as cdl_sunflowers,
    CDL_PEANUTS as cdl_peanuts,
    CDL_BARLEY as cdl_barley,
    CDL_DURUM_WHEAT as cdl_durum_wheat,
    CDL_SPRING_WHEAT as cdl_spring_wheat,
    CDL_WINTER_WHEAT as cdl_winter_wheat,
    CDL_WWHT_SOY_DBL_CROP as cdl_wwht_soy_dbl_crop,
    CDL_OATS as cdl_oats,
    CDL_ALFALFA as cdl_alfalfa,
    CDL_OTHER_HAYS as cdl_other_hays,
    CDL_DRY_BEANS as cdl_dry_beans,
    CDL_POTATOES as cdl_potatoes,
    CDL_FALLOW_IDLE as cdl_fallow_idle,
    CDL_PASTURE_GRASS as cdl_pasture_grass,
    CDL_ORANGES as cdl_oranges,
    CDL_OTHER_CROPS as cdl_other_crops,
    CDL_ALL_OTHER_LAND as cdl_all_other_land
from {{ ref('attributes_gageii_LC_Crops') }}

