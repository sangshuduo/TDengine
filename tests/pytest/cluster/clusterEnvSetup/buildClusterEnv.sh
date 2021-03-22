#!/bin/bash
echo "Executing buildClusterEnv.sh"
CURR_DIR=`pwd`

if [ $# != 6 ]; then 
  echo "argument list need input : "
  echo "  -n numOfNodes"
  echo "  -v version"
  echo "  -d docker dir" 
  exit 1
fi

NUM_OF_NODES=
VERSION=
DOCKER_DIR=
while getopts "n:v:d:" arg
do
  case $arg in
    n)
      NUM_OF_NODES=$OPTARG
      ;;
    v)
      VERSION=$OPTARG
      ;;
    d)
      DOCKER_DIR=$OPTARG
      ;;    
    ?)
      echo "unkonwn argument"
      ;;
  esac
done

function addTaoscfg {
  for i in {1..5}
  do 
    touch $DOCKER_DIR/node$i/cfg/taos.cfg
    echo 'firstEp          tdnode1:6030' > $DOCKER_DIR/node$i/cfg/taos.cfg
    echo 'fqdn             tdnode'$i >> $DOCKER_DIR/node$i/cfg/taos.cfg
    echo 'arbitrator       tdnode1:6042' >> $DOCKER_DIR/node$i/cfg/taos.cfg
  done
}

function createDIR {
  for i in {1..5}
  do    
    mkdir -p $DOCKER_DIR/node$i/data
    mkdir -p $DOCKER_DIR/node$i/log
    mkdir -p $DOCKER_DIR/node$i/cfg
    mkdir -p $DOCKER_DIR/node$i/core
  done
}

function cleanEnv {
  echo "Clean up docker environment"    
  for i in {1..5}
  do    
    rm -rf $DOCKER_DIR/node$i/data/*    
    rm -rf $DOCKER_DIR/node$i/log/*
  done
}

function prepareBuild {

  if [ -d $CURR_DIR/../../../../release ]; then
    echo release exists
    rm -rf $CURR_DIR/../../../../release/*
  fi

  if [ ! -e $DOCKER_DIR/TDengine-server-$VERSION-Linux-x64.tar.gz ] || [ ! -e $DOCKER_DIR/TDengine-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
    cd $CURR_DIR/../../../../packaging
    echo "generating TDeninger packages"
    ./release.sh -v edge -n $VERSION >> /dev/null

    if [ ! -e $CURR_DIR/../../../../release/TDengine-server-$VERSION-Linux-x64.tar.gz ]; then
      echo "no TDengine install package found"
      exit 1
    fi

    if [ ! -e $CURR_DIR/../../../../release/TDengine-arbitrator-$VERSION-Linux-x64.tar.gz ]; then
      echo "no arbitrator install package found"
      exit 1
    fi

    cd $CURR_DIR/../../../../release
    mv TDengine-server-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
    mv TDengine-arbitrator-$VERSION-Linux-x64.tar.gz $DOCKER_DIR
  fi
  
  rm -rf $DOCKER_DIR/*.yml
  cd $CURR_DIR

  cp *.yml  $DOCKER_DIR
  cp Dockerfile $DOCKER_DIR
}

function clusterUp {
  echo "docker compose start"
  
  cd $DOCKER_DIR  

  if [ $NUM_OF_NODES -eq 2 ]; then
    echo "create 2 dnodes"
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=TDengine-arbitrator-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION DIR2=TDengine-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose up -d
  fi

  if [ $NUM_OF_NODES -eq 3 ]; then
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=TDengine-arbitrator-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION DIR2=TDengine-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml -f node3.yml up -d
  fi

  if [ $NUM_OF_NODES -eq 4 ]; then
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=TDengine-arbitrator-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION DIR2=TDengine-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml -f node3.yml -f node4.yml up -d
  fi

  if [ $NUM_OF_NODES -eq 5 ]; then
    PACKAGE=TDengine-server-$VERSION-Linux-x64.tar.gz TARBITRATORPKG=TDengine-arbitrator-$VERSION-Linux-x64.tar.gz DIR=TDengine-server-$VERSION DIR2=TDengine-arbitrator-$VERSION VERSION=$VERSION DATADIR=$DOCKER_DIR docker-compose -f docker-compose.yml -f node3.yml -f node4.yml -f node5.yml up -d
  fi

  echo "docker compose finish"
}

createDIR
cleanEnv
addTaoscfg
prepareBuild
clusterUp