# workshop-sparkstreaming


    
# 开发环境准备

```
java:1.8
scala:2.12.8
```

## 安装zookeeper
```
brew install zookeeper
安装后路径：/usr/local/Cellar/zookeeper
配置文件路径：/usr/local/etc/zookeeper
brew services start zookeeper
```

## 安装kafka
```
brew install kafka
安装后路径：/usr/local/Cellar/kafka
配置文件目录：/usr/local/etc/kafka
brew services start kafka
```

## 创建topic

```
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic  tw-workshop 
kafka-topics --list --zookeeper localhost:2181

```
kafka-console-producer.sh --broker-list localhost:9092 --topic tw-workshop
kafka-console-consumer --bootstrap-server localhost:9092 --topic tw-workshop
