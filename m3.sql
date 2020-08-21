drop table if exists t_status;
drop table if exists t_profile;
drop table if exists testing;
create table testing (
	tid timestamp,
	useless bool
);
create table t_profile (
	ts timestamp,
	tid bigint,
    id bigint,
    child binary(8)
);
create table if not exists t_status (
	ts timestamp,
	tid bigint,
	status bool
);
