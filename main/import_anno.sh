#----
#---- Downloads annotated data from gcs and imports into mosey database
#----

eval "$(docopts -h - : "$@" <<EOF
Usage: import_anno.sh [options] <argv> ...
Options:
			--help     Show help options.
			--version  Print program version.
			--table=<table>    Table to load the annotations into. Matches using event_id.
			--clean=<clean>   If true, deletes intermediate csv files. defaults to true
----
import_anno.sh 0.1
EOF
)"


#TODO: make a test to make sure group subsetting is happening correctly
#   could set group size to small value, use a small dataset, make sure values are updated from both 
#   groups

#gcsOutURL=gs://${gcsBucket}/${gcsP} #This is the url to the output folder (includes bucket)
#annoP=data/anno/annotated
#db=~/projects/ms3/analysis/full_workflow_poc/data/mosey.db

gcsOutURL=${argv[0]}
annoP=${argv[1]}
db=${argv[2]}

#TODO: pass in as a single value, not an array. For multiple, use envs.csv
#envs=(${argv[3]})

#Set defaults
[[ -z "$table" ]] && table="event"
[[ -z "$clean" ]] && clean="true"

# echo $gcsOutURL
# echo $annoP
# echo $db
# echo ${envs[@]}

mkdir -p $annoP

#---- Load variables from control files

#study.csv
studyIds=($(mlr --csv --opprint filter '$run == 1' then cut -f study_id ctfs/study.csv | tail -n +2))

#envs.csv
envs=($(mlr --csv --opprint filter '$run == 1' then cut -f env_id ctfs/env.csv | tail -n +2))
colnames=($(mlr --csv --opprint filter '$run == 1' then cut -f col_name ctfs/env.csv | tail -n +2))

#Remove \r suffix
studyIds=( ${studyIds[@]%$'\r'} )
envs=( ${envs[@]%$'\r'} )
colnames=( ${colnames[@]%$'\r'} )

for studyId in "${studyIds[@]}"
do
	echo "*******"
	echo "Start processing study ${studyId}"
	echo "*******"
	
	#studyId=10763606 #LifeTrack White Stork Poland (419 rows)
	#studyId=${studyIds[0]}
	
	# get length of an array
  n=${#envs[@]}

  # use for loop to read all values and indexes
  for (( i=0; i<${n}; i++ ));
  do
  
	# for env in "${envs[@]}"
	# do
	#  envN=${env##*/} #gets the name (w/o path) of the env variable
	#  echo "Importing $envN"
	#   annoN=${studyId}_${envN}
		
		
    #i=0
    
		#env=users/benscarlson/projects/ms3/dist2forest

    echo "Importing ${colnames[$i]}"
    
		annoN=${studyId}_${colnames[$i]}
		annoPF=$annoP/${annoN}.csv
		gcsCSV=$gcsOutURL/${annoN}.csv

		#check here if file exists and skip if it doesn't exist
		#https://stackoverflow.com/questions/48676712/how-to-check-if-any-given-object-exist-in-google-cloud-storage-bucket-through-ba
		
		#gsutil ls $gcsOutURL/${annoN}_*.csv
		gsutil -q stat $gcsOutURL/${annoN}_*.csv

		return_value=$? #returns 0 if files exist, 1 if there are no results

		if [ $return_value = 1 ]; then
			echo "$gcsCSV does not exist. Skipping."
			continue
		fi
		
		echo "Downloading $gcsOutURL/${annoN}_*.csv ..."
		
		gsutil cp $gcsOutURL/${annoN}_*.csv $annoP

    echo "Merging individual task files..."
    awk '(NR == 1) || (FNR > 1)' $annoP/${annoN}_*.csv > $annoPF
    
		echo Updating the database...
		#Note that .import loads empty fields in csv as "", not NULL
		# So need to explicitly set these as null after loading
		# 
		#Below it is very important for EOF to have tabs instead of spaces
		#Can't get rstudio to insert tabs instead of spaces
		#So any change to the tabs in the here file need to redo tabs in sublime text
		# Open in Sublime Text. Click on "Tab Size: X" bottom right. Select "convert indentation to tabs"
		sqlite3 $db <<-EOF
			begin;
			.mode csv temp_annotated

			.import $annoPF temp_annotated

			update $table
			set ${colnames[$i]} = t.${colnames[$i]}
			from temp_annotated t
			where t.anno_id = ${table}.event_id;

			update $table set ${colnames[$i]} = NULL where ${colnames[$i]}='';

			drop table temp_annotated;

			end;
		EOF

		#---- Cleanup
		
    if [ $clean = "true" ]; then
      echo "Deleting temporary csv files."
      rm -f $annoP/${annoN}_*.csv
  		rm -f $annoPF
  	else
  	  echo "Did not delete intermediate csv files."
  	fi

		echo ${colnames[$i]} complete

	done
done

echo "Script complete"