# Installation
## Zeppelin
- download Spark: https://www.apache.org/dyn/closer.lua/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
- download Zeppelin: http://www.apache.org/dyn/closer.cgi/zeppelin/zeppelin-0.8.0/zeppelin-0.8.0-bin-all.tgz
```bash
cd /opt/zeppelin-0.8.0-bin-all/conf
mv zeppelin-env.sh.template zeppelin-env.sh
vi zeppelin-env.sh
export SPARK_HOME=/opt/spark-2.3.1-bin-hadoop2.7
```
