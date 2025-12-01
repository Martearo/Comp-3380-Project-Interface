
build: SQLServerDemo.class

SQLServerDemo.class: SQLServerDemo.java
	javac SQLServerDemo.java

run: SQLServerDemo.class
	java -cp .:mssql-jdbc-11.2.0.jre11.jar SQLServerDemo

clean:
	rm SQLServerDemo.class
