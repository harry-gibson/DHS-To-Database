grant CONNECT  on database dhs_data_pit to dhs_reader;
grant usage on schema dhs_data_tables to dhs_reader;
grant usage on schema dhs_data_locations to  dhs_reader;
grant usage on schema dhs_survey_specs to dhs_reader;

grant select on all tables in schema dhs_data_tables to dhs_reader;
grant select on all tables in schema dhs_data_locations to dhs_reader;
grant select on all tables in schema dhs_survey_specs to dhs_reader;

alter default privileges in schema dhs_data_tables grant select on TABLES to dhs_reader;
alter default privileges in schema dhs_data_locations grant select on TABLES to dhs_reader;
alter default privileges in schema dhs_survey_specs grant select on TABLES to dhs_reader;