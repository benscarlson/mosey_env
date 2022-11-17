#!/bin/zsh

#----
#---- Downloads annotated data from gcs and imports into mosey database
#----

eval "$(docopts -h - : "$@" <<EOF
Usage: import_anno.sh [options] <argv> ...
Options:
			--help     Show help options.
			--version  Print program version.
			--table=<table>    Table to load the annotations into. Matches using event_id.
			--clean=<clean>   If true, deletes intermediate csv files. defaults to true.
----
import_anno.sh 0.1
EOF
)"


#TODO: make a test to make sure group subsetting is happening correctly
#   could set group size to small value, use a small dataset, make sure values are updated from both 
#   groups

# pd=~/projects/ms2
# wd=$pd/analysis/poc/mosey_env/mosey_env1
# cd $wd
# 
# gcsOutURL=gs://mol-playground/benc/projects/ms2/poc/mosey_env/mosey_env1/anno #This is the url to the output folder (includes bucket)
# annoP=data/anno
# db=$pd/analysis/main/data/mosey.db

gcsOutURL=${argv[1]}
annoP=${argv[2]}
db=${argv[3]}

echo gcsOutURL: $gcsOutURL
echo annoP: $annoP
echo db: $db

#TODO: pass in as a single value, not an array. For multiple, use envs.csv
#envs=(${argv[3]})

#Set defaults
[[ -z "$table" ]] && table="event"
[[ -z "$clean" ]] && clean="true"

# echo $gcsOutURL
# echo $annoP
# echo $db

#TODO: pass in "rollback", if true, set endTx=rollback. else if empty endTx=commit
endTx=commit
#endTx=rollback
		
mkdir -p $annoP

#---- Load variables from control files

entity=population

#study.csv
#studyIds=($(mlr --csv --opprint filter '$run == 1' then cut -f study_id ctfs/$entity.csv | tail -n +2))
names=($(mlr --csv --opprint filter '$run == 1' then cut -f name ctfs/$entity.csv | tail -n +2))

echo $names[@]

#envs.csv
envs=($(mlr --csv --opprint filter '$run == 1' then cut -f env_id ctfs/env.csv | tail -n +2))
colnames=($(mlr --csv --opprint filter '$run == 1' then cut -f col_name ctfs/env.csv | tail -n +2))

echo $envs[@]
echo $colnames[@]

#Remove \r suffix
#NOTE: I don't think I need these anymore when using zsh
# studyIds=( ${studyIds[@]%$'\r'} )
# envs=( ${envs[@]%$'\r'} )
# colnames=( ${colnames[@]%$'\r'} )

for name in "${names[@]}"
do
	echo "*******"
	echo "Start processing $entity $name"
	echo "*******"
	
	# get length of an array
  n=${#envs[@]}

  #NOTE zsh starts at 1! updated the loop, test
  # use for loop to read all values and indexes
  for (( i=1; i<=${n}; i++ ));
  do

		#Note zsh arrays start at 1!
    #i=1

    echo "Importing ${colnames[$i]}"
    
		annoN=${name}_${colnames[$i]}
		annoPF=$annoP/${annoN}.csv
		gcsCSV=$gcsOutURL/${annoN}.csv

		#check here if file exists and skip if it doesn't exist
		#https://stackoverflow.com/questions/48676712/how-to-check-if-any-given-object-exist-in-google-cloud-storage-bucket-through-ba
		
		#gsutil ls $gcsOutURL/${annoN}_*.csv
		#It seems zsh attemps to expand the * before sending to gsutil.
		# Wrap this in '' so that shell will not expand it
		gsutil -q stat $gcsOutURL/${annoN}_'*'.csv

		return_value=$? #returns 0 if files exist, 1 if there are no results

		if [ $return_value = 1 ]; then
			echo "$gcsCSV does not exist. Skipping."
			continue
		fi
		
		echo "Downloading $gcsOutURL/${annoN}_*.csv ..."
		
		gsutil cp $gcsOutURL/${annoN}_'*'.csv $annoP

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
		
    #annoying but syntax highlighting does not like the set syntax
		
		echo Transaction will $endTx
		
		sqlite3 $db <<-EOF
			begin;
			.mode csv temp_annotated

			.import $annoPF temp_annotated

			# update $table
			# set ${colnames[$i]} = t.${colnames[$i]}
			# from temp_annotated t
			# where t.anno_id = ${table}.event_id;
      
			update $table
			set (${colnames[$i]}, ${colnames[$i]}_anno) = (t.${colnames[$i]},1)
			from temp_annotated t
			where t.anno_id = ${table}.event_id;
			
			# TODO: it is safter to set "" to null in temp table
			#   This is because any function applied to the value might return 0 for ""
			update $table set ${colnames[$i]} = NULL where ${colnames[$i]}='';

			drop table temp_annotated;

			${endTx};
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