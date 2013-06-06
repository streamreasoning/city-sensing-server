abspath=$(cd ${0%/*} && echo $PWD/${0##*/})
path_only=`dirname "$abspath"`
cd $path_only
java -Xms512m -Xmx1g -jar city_sensing_server-0.2.jar