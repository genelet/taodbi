drop table if exists at_status;
drop table if exists at_profile;
drop table if exists atesting;
create table atesting (
	id timestamp,
	x binary(8),
	y binary(8)
);
create table at_profile (
	ts timestamp,
	id bigint,
    x binary(8),
    y binary(8),
    z binary(8)
);
create table if not exists at_status (
	ts timestamp,
	id bigint,
	status bool
);
