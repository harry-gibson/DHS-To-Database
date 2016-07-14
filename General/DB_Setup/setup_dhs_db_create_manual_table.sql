DROP TABLE dhs_data_tables."REC91";
CREATE TABLE dhs_data_tables."REC91" (
surveyid character varying(3),
caseid character varying(18),
squestid character varying(4),
data jsonb
)
WITH (
	OIDS=TRUE
);

DROP TABLE dhs_data_tables."REC94";
CREATE TABLE dhs_data_tables."REC94" (
surveyid character varying(3),
caseid character varying(18),
idx94 character varying(2),
rec94idx character varying(2),
data jsonb
)
WITH (
	OIDS=TRUE
);

DROP TABLE dhs_data_tables."REC95";
CREATE TABLE dhs_data_tables."REC95" (
surveyid character varying(3),
caseid character varying(18),
idx95 character varying(2),
data jsonb
)
WITH (
	OIDS=TRUE
);

DROP TABLE dhs_data_tables."REC97";
CREATE TABLE dhs_data_tables."REC97" (
surveyid character varying(3),
caseid character varying(15),
idx97 character varying(2),
pidx97 character varying(2),
bidx97 character varying(2),
data jsonb
)
WITH (
	OIDS=TRUE
);

DROP TABLE dhs_data_tables."RECH3";
CREATE TABLE dhs_data_tables."RECH3" (
surveyid character varying(3),
hhid character varying(15),
data jsonb
)
WITH (
	OIDS=TRUE
);

