all:	join.jar

org:
	javac -classpath /usr/lib/hadoop/hadoop-core.jar -d . *.java

join.jar:	org
	jar -cvf join.jar -C . .

run: join.jar org
	hadoop jar join.jar org.myorg.JoinMain file01 file02 output 

clean:
	rm -rf org join.jar output

