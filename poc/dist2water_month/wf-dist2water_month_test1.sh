wd=~/projects/mosey_env/analysis/poc/dist2water_month_test1
src=~/projects/mosey_env/src

mkdir -p $wd
cd $wd

pyenv activate gee

env=src/main/computed_layers/dist2water_month.r
dat=users/benscarlson/projects/ms3/tracks/8863543
out=benc/projects/mosey_env/annotated/dist2water_month_test1/8863543

$src/main/anno_gee.r $dat $env $out -c 'dist2water_month' -g 0 -n 10

#full dataset
$src/main/anno_gee.r $dat $env $out -c 'dist2water_month'
