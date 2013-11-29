rm proj4d.jar;

export PRIVATE_SERVER='128.237.64.120';

wget ${PRIVATE_SERVER}/server/proj4d.jar;

hadoop jar proj4d.jar com.yzong.ccproj4.PopulateDB -b CorpusTable -n 8 -t 5;

