export LANG="en_US.UTF-8"
export JAVA_HOME=/usr/local/java
export PATH=$PATH:$JAVA_HOME/bin
export CLASSPATH=.:$JAVA_HOME/lib/*.jar:$JAVA_HOME/jre/lib/*.jar

export JAVA_OPTS='-Xmx4096m -Xms1024m -Dfile.encoding=UTF-8'

export APP_NAME='STREAMING_AGENT'