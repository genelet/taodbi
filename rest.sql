drop table if exists tstatus;
drop table if exists tprofile;
drop table if exists tmain;
create table if not exists tmain (
	id timestamp,
	username binary(16)
);
create table if not exists tprofile (
	ts timestamp,
	id bigint,
	username binary(16),
	passwd binary(32),
	firstname binary(16),
	lastname binary(16),
	gender bool,
	street binary(32),
	city binary(32),
	province tinyint,
	phone binary(16),
	email binary(32)
);
create table if not exists tstatus (
	ts timestamp,
	id bigint,
	status bool
);
