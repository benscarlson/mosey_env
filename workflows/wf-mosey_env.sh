pyenv activate gee

wd=~/projects/ms3/analysis/anno/
src=~/projects/ms3/src
export SRC=$src #for mosey_anno_gee.sh
db=~/projects/ms3/analysis/full_workflow_poc/data/mosey.db

cd $wd

#TODO: I may not need gcsInP, but just gcsInURL
geePtsP=users/benscarlson/projects/ms3/tracks #folder holding the gee point datasets
gcsBucket=mol-playground
gcsInP=benc/projects/ms3/ingest_gee #This holds the csvs that will be imported to gee
gcsOutP=benc/projects/ms3/annotated #This is the output folder for annotated csvs (excluding bucket)
csvP=data/anno/ingest_gee #local folder that holds the csv files to be ingested into gee
annoP=data/anno/annotated #local folder that holds the annotated csv files
sesid=full_wf

gcsInURL=gs://${gcsBucket}/${gcsInP}
gcsOutURL=gs://${gcsBucket}/${gcsOutP} #This is the url to the output folder (includes bucket)

envs=(
  projects/map-of-life/benc/projects/ms3/dist2water_perm 
  users/benscarlson/projects/ms3/dist2forest
  projects/map-of-life/benc/projects/ms3/dist2urban
)

#----
#---- Update the database structure ----#
#----

sqlite3 $db "alter table event add column dist2forest REAL;"
sqlite3 $db "alter table event add column dist2water_perm REAL;"
sqlite3 $db "alter table event add column dist2urban REAL;"

#Make a temp table to test, b/c event table is way to slow to check results
# sqlite3 $db "drop table if exists event_test"
# 
# sqlite3 $db "create table event_test as select * from event \
# where study_id in (10763606, 8863543, 24442409, 10763606, 9493881)"

#----
#---- Import studies into GEE
#----

/usr/bin/time $src/poc/anno/gee_event_import.sh $geePtsP $gcsInURL $db $csvP $sesid

#----
#---- Annotate 
#----

#Don't run this until all import tasks have finished
#https://code.earthengine.google.com/tasks

/usr/bin/time $src/poc/anno/mosey_anno_gee.sh $geePtsP $gcsOutP "${envs[*]}"

#----
#---- Import into mosey ----#
#----

$src/poc/anno/import_anno.sh $gcsOutURL $annoP $db "${envs[*]}"
