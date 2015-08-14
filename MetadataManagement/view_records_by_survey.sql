SELECT q.dhs_file_code, q.record_code, q.record_desc, i.recode, i.recode_description
FROM
dhs_recodes i INNER JOIN (
	select s.dhs_file_code, t.record_code, t.record_desc, t.id from dhs_records t INNER JOIN dhs_surveys s ON t.dhs_file_code = s.dhs_file_code ) q 
ON i.record_id = q.id
WHERE q.record_code ='RECML'