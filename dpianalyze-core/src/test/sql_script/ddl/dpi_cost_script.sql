create table if not exists dm_dpi_master.carrierSide_cost
(
    load_day     string,
    day          string,
    id_cnt       int,
    dup_id_cnt   int,
    cal_cnt      int,
    carrier_cost DOUBLE
)
    partitioned by (month string, source string)
    stored as orc;

create table if not exists dm_dpi_master.cateSide_cost
(
    load_day     string,
    day          string,
    plat         string,
    cate_l1      string,
    tag_cnt      int,
    dup_tag_cnt  int,
    cal_cnt      int,
    cate_l1_cost DOUBLE
)
    partitioned by (month string, source string)
    stored as orc;

create table if not exists dm_dpi_master.platSide_cost
(
    load_day           string,
    day                string,
    plat               string,
    tag_cnt            int,
    dup_tag_cnt        int,
    plat_rate          DOUBLE,
    plat_cal_cost      DOUBLE,
    cal_cnt            int,
    plat_cost          DOUBLE,
    last_plat_rate     DOUBLE,
    last_plat_cal_cost DOUBLE
)
    partitioned by (month string, source string)
    stored as orc;

create table if not exists dm_dpi_master.plat_distribution
(
    source string,
    load_day string,
    day string,
    plat string,
    plat_tag_cnt int,
    plat_curr_sum int,
    carrier_curr_sum int,
    max_plat_curr_sum int,
    max_carrier_curr_sum int
)
    stored as orc;