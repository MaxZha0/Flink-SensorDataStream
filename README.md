***flink程序模板，编写flink程序时将此仓库fork出去，或者新建一个与自身业务相关的分支***

### 修改项
```
1. 修改pom文件中的<artifactId>flink-template</artifactId>, 将flink-template该问业务相关的名字
2. 修改script/shell/send.sh 中的jar包名称 
```

### 打包
``` mvn clean package -DskipTests ```

### 发布

``` 
sh script/shell/send.sh
 ```

### 启动

```
./bin/flink run -m yarn-cluster  -yjm 1024 -ytm 1024 -c com.bainan.test.PicCv /home/bigdata/flinkApp/flinkTemplate/flink-template-1.0-SNAPSHOT-jar-with-dependencies.jar
将其中的jar包替换成自身jar包名称， -c 后跟着的是Main函数的全类名也需要替换
```
