LIB_PATH=/home/phao3/protobuf/protobuf-3.4.0/java/core/target/protobuf.jar
all: clean
	mkdir bin
	mkdir bin/server_classes
	mkdir bin/controller_classes
	javac -classpath $(LIB_PATH) -d bin/branch_classes/ src/Branch.java src/Bank.java src/Channel.java
	javac -classpath $(LIB_PATH) -d bin/controller_classes/ src/Controller.java src/Bank.java src/FileChecker.java src/FileProcessor.java


clean:
	rm -rf bin/

