#---- Check results ----#
sqlite3 $db
.headers on
select * from event_test where study_id = 10763606 and dist2forest is not null limit 5;
select * from event_test dist2forest is not null limit 5;

select study_id, count(*) as num from event_test 
where dist2forest is not nullgroup by study_id;

select study_id, count(*) as num from event_test 
where dist2water_perm is not null group by study_id;

#---- Delete/reset everything ---#
#
#Make sure these variables are set!!
echo rm -rf $csvP/*.csv
echo rm -rf $annoP/*.csv

pts=$( earthengine ls $geePtsP )
earthengine rm $pts --dry_run 

gsutil ls $gcsInURL
gsutil rm $gcsInURL/*

gsutil ls $gcsOutURL
gsutil rm $gcsOutURL/*

#reset envs
sqlite3 $db "update event_test set dist2forest=null, dist2water_perm=null"