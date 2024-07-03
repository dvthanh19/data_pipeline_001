# STREAMING DATA FROM FILE TO KAFKA

**Start date**: 02/07/2024  
**Description**: ...  


## Prequesites
- Install protoc at [link](https://github.com/protocolbuffers/protobuf/releases/tag/v27.2)

## Getting started
- Create file *pom.xml*
- Create *file_schema.proto*

Run the docker up for Kafka broker and UI
```shell
docker compose -f docker-compose.yml -p demo up -d
```

Run the command
```shell
# protoc --proto_path=<> --java_out=<output dir> <proto_file_path>
protoc --proto_path=./src/main/java/protobuf --java_out=./src/main/java/protobuf/ ./src/main/java/protobuf/file_schema.proto
# D:\protoc-27.2-win64\bin\protoc --proto_path=./src/main/java/protobuf --java_out=./src/main/java/protobuf/ ./src/main/java/protobuf/file_schema.proto
```

Then go inside *FileSchema.java* created after the above command and add package for it


## Conclusion
