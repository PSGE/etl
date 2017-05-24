#!/bin/bash
year=2017
month=05
day=01

date='2017-05-01'

#fl_bs="/google/data/ro/projects/cloud/bigstore/fileutil_bs"
fl_bs="/google/src/cloud/gfr/fileutil/google3/blaze-bin/cloud/bigstore/util/fileutil_bs"
tar=${fl_bs}" tar -R "

earfiles="/namespace/m-lab/archive/${date}/*.ear"
earfiles_pattern="/namespace/m-lab/archive/${date}/(.*)measurement-lab.org-(.*).ear"
dest="/bigstore/gfr/${date}/"

echo $earfiles
echo $earfiles_pattern
echo $dest

set +e
#set -x

i=0

#regex='^/namespace/m-lab/archive/2017-05-01/rsync-20170501-([a-z]+)\.mlab\.(mlab\d\.[a-z0-9]{5})\.measurement-lab\.org-([a-z]+)\.ear\$'
for x in `fileutil ls $earfiles`
do
  if [[ $x =~ $earfiles_pattern ]]
  then
    fn="${BASH_REMATCH[1]}"
    kind="${BASH_REMATCH[2]}"
    echo sem -j 10 $tar "${dest}${kind}/${fn}tar" "/ear$x"
    sem -j 10 $tar "${dest}${kind}/${fn}tar" "/ear$x"
  else
    echo $x
    exit 1
  fi
done

sem --wait

exit 0


/ear/namespace/m-lab/archive/2017-05-01/rsync-20170501-ndt.iupui.mlab1.lax04.measurement-lab.org-ndt.ear/2017/05/01/20170501T20:32:22.413693000Z_adsl-162-196-13-221.lightspeed.irvnca.sbcglobal.net:61578.s2c_snaplog.gz

year=2016
month=11
for i in {1..30}
  do
    day=$( printf '%02d' $i )
    fileutil cp /namespace/m-lab/archive/${year}-${month}-${day}/*side* /cns/qb-d/home/m-lab/embargo_ear --gfs_user=m-lab
    mkdir /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/${year}/${month}/${day}
    fileutil mkdir /cns/qb-d/home/m-lab/embargo_output/${year}/${month}/${day} --gfs_user=m-lab

    for x in `fileutil ls /cns/qb-d/home/m-lab/embargo_ear/*.ear`
      do
        site=`echo "${x}" | awk '{ print substr( $0, 60, 11 ) }'`
        echo "${site}"

        mkdir /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/${year}/${month}/${day}/${site}

        fileutil cp /ear/${x}/${year}/${month}/${day}/${site}/*.gz  /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/${year}/${month}/${day}/${site}/
        for y in `ls /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/${year}/${month}/${day}/${site}/*.gz`
          do
            gunzip "${y}"
          done
        cd /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp

        name=`echo "${site}" | tr '.' '-'`
        tar czf ./${year}${month}${day}T000000Z-${name}-sidestream-0000.tgz *
        fileutil cp -f ${year}${month}${day}T000000Z-${name}-sidestream-0000.tgz /cns/qb-d/home/m-lab/embargo_output/${year}/${month}/${day}/ --gfs_user=m-lab
        rm ${year}${month}${day}T000000Z-${name}-sidestream-0000.tgz
        rm /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/*/*/*/*/*
        rmdir /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/${year}/${month}/${day}/${site}
      done
    rmdir /usr/local/google/home/yachang/go_pipe/src/github.com/yachang/hello/embargo_tmp/${year}/${month}/${day}
    fileutil rm -f /cns/qb-d/home/m-lab/embargo_ear/* --gfs_user=m-lab
  done

