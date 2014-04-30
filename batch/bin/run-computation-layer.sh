#!/bin/bash
# Copyright Myrrix Ltd.

# This is a utility script which may be set with appropriate values, to avoid re-typing them every time
# on the command line. It illustrates usage as well.

# Set these to the data, time thresholds desired, if applicable
#DATA_THRESHOLD=
#TIME_THRESHOLD=

# Set these to the instance and bucket you will run the Computation Layer for
#INSTANCE_ID=myinstance
#BUCKET=mybucket

# Uncomment these to make recommendation or compute item-item similarities too
#RECOMMEND=true
#ITEM_SIMILARITY=true
#CLUSTER=true

# Uncomment to force one generation run at startup
#FORCE_RUN=true

# Set to the port to listen for HTTP requests. This or SECURE_PORT must be set.
PORT=8080

# Set to listen for HTTPS connections on the given port. This or PORT must be set.
#SECURE_PORT=8443

# Set the keystore file to enable SSL / HTTPS, and supply the password if needed.
#KEYSTORE_FILE=/path/to/keystore
#KEYSTORE_PASSWORD=password

# Set these values if the Serving Layer requires a username and password to access
#USERNAME=username
#PASSWORD=password
#CONSOLE_ONLY_PASSWORD=true

# Set any other args here
#OTHER_ARGS=

# Set any system properties here
#SYS_PROPS=-Dmapred.reduce.tasks=1


# -------- Apache Hadoop-specific settings

# If using an Apache Hadoop cluster, set HADOOP_CONF_DIR here (if it is not already set in the environment)
# HADOOP_HOME also works.
#export HADOOP_CONF_DIR=/path/to/hadoop/conf

# -------- Amazon EMR settings

# Set your AWS access key and secret key here
#AWS_ACCESS_KEY=...
#AWS_SECRET_KEY=...


# ----- Nothing to set below here -------

ALL_ARGS=""
if [ -n "${DATA_THRESHOLD}" ]; then
  ALL_ARGS="${ALL_ARGS} --data=${DATA_THRESHOLD}"
fi

if [ -n "${TIME_THRESHOLD}" ]; then
  ALL_ARGS="${ALL_ARGS} --time=${TIME_THRESHOLD}"
fi

if [ -n "${INSTANCE_ID}" ]; then
  ALL_ARGS="${ALL_ARGS} --instanceID=${INSTANCE_ID}"
fi

if [ -n "${BUCKET}" ]; then
  ALL_ARGS="${ALL_ARGS} --bucket=${BUCKET}"
fi

if [ -n "${RECOMMEND}" ]; then
  ALL_ARGS="${ALL_ARGS} --recommend"
fi

if [ -n "${ITEM_SIMILARITY}" ]; then
  ALL_ARGS="${ALL_ARGS} --itemSimilarity"
fi

if [ -n "${CLUSTER}" ]; then
  ALL_ARGS="${ALL_ARGS} --cluster"
fi

if [ -n "${FORCE_RUN}" ]; then
  ALL_ARGS="${ALL_ARGS} --forceRun"
fi

if [ -n "${PORT}" ]; then
  ALL_ARGS="${ALL_ARGS} --port=${PORT}"
fi
if [ -n "${SECURE_PORT}" ]; then
  ALL_ARGS="${ALL_ARGS} --securePort=${SECURE_PORT}"
fi

if [ -n "${KEYSTORE_FILE}" ]; then
  ALL_ARGS="${ALL_ARGS} --keystoreFile=${KEYSTORE_FILE} --keystorePassword=${KEYSTORE_PASSWORD}"
fi

if [ -n "${USERNAME}" ]; then
  ALL_ARGS="${ALL_ARGS} --userName=${USERNAME}"
fi
if [ -n "${PASSWORD}" ]; then
  ALL_ARGS="${ALL_ARGS} --password=${PASSWORD}"
fi
if [ -n "${CONSOLE_ONLY_PASSWORD}" ]; then
  ALL_ARGS="${ALL_ARGS} --consoleOnlyPassword"
fi

if [ -n "${OTHER_ARGS}" ]; then
  ALL_ARGS="${ALL_ARGS} ${OTHER_ARGS}"
fi

ALL_SYS_PROPS="";
if [ -n "${AWS_ACCESS_KEY}" ]; then
  ALL_SYS_PROPS="${ALL_SYS_PROPS} -Dstore.aws.accessKey=${AWS_ACCESS_KEY} -Dstore.aws.secretKey=${AWS_SECRET_KEY}"
fi
if [ -n "${SYS_PROPS}" ]; then
  ALL_SYS_PROPS="${ALL_SYS_PROPS} ${SYS_PROPS}"
fi


COMPUTATION_JAR=`ls myrrix-computation-*.jar`

java ${ALL_SYS_PROPS} -jar ${COMPUTATION_JAR} ${ALL_ARGS} $@
