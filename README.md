# Log Analysis With Scala

This sbt scala project which analyses log files

## Package

To package your project:
```bash
sbt assembly
```
![](cdn/1.png)

## Deploy 

Copy/Upload the fatjar to the destination
```
TARGET_LOCATION=<location>
cp target/scala-2.12/spark-sbt-template-assembly-1.0.jar $TARGET_LOCATION
```

![](cdn/2.png)


## Run

To run your project locally:
```
JAR_PATH=$(pwd)/target/scala-2.12/spark-sbt-template-assembly-1.0.jar
spark-submit --master=local[*] --deploy-mode client --class App $JAR_PATH
```

![](cdn/3.png)


## To run on IntelliJ

Make sure you include the app.run file as follows.

```
<component name="ProjectRunConfigurationManager">
  <configuration default="false" name="App" type="Application" factoryName="Application">
    <option name="ALTERNATIVE_JRE_PATH" value="11" />
    <option name="ALTERNATIVE_JRE_PATH_ENABLED" value="true" />
    <option name="INCLUDE_PROVIDED_SCOPE" value="true" />
    <option name="MAIN_CLASS_NAME" value="App" />
    <module name="SparkLogAnalysis" />
    <option name="PROGRAM_PARAMETERS" value="-s src/res/access.log.gz -r src/res/reports" />
    <option name="VM_PARAMETERS" value="-Dspark.master=local[*]" />
    <method v="2">
      <option name="Make" enabled="true" />
    </method>
  </configuration>
</component>
```
## To Deploy on AWS

### Upload resources

You need to upload the access log file and the generate fatjar to s3.

![](cdn/aws_upload_access_s3.png)

### EMR Cluster

Start an EMR cluster

![](cdn/emr1.png)

![](cdn/emr2.png)


### Add the jar file to the cluster with arguments specified
![](cdn/step3.png)

Specify the arguments as below.

```
-s s3://dstilogrepo/access.log.gz -r s3://dstilogrepo/reports
```




