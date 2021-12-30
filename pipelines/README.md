## running tests

```
./gradlew clean test -i
docker run -it -u gradle -v `pwd`:/dtest -w /dtest/pipelines gradle:7.3.3-jdk11-alpine gradle clean test -i
```
