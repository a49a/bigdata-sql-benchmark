
all: target/lib/dsdgen.jar target/hive-tpcds-setup-0.1-SNAPSHOT.jar

target/hive-tpcds-setup-0.1-SNAPSHOT.jar: $(shell find . -name *.java)
	mvn package

target/tpcds_kit.zip: tpcds_kit.zip
	mkdir -p target/
	cp tpcds_kit.zip target/tpcds_kit.zip

tpcds_kit.zip:
	curl http://public-repo-1.hortonworks.com/hive-testbench/tpcds/README
	[[ -e tpcds_kit.zip ]] || curl --output tpcds_kit.zip https://public-repo-1.hortonworks.com/hive-testbench/tpcds/TPCDS_Tools.zip

target/lib/dsdgen.jar: target/tools/dsdgen
	cd target/; mkdir -p lib/; ( jar cvf lib/dsdgen.jar tools/ || gjar cvf lib/dsdgen.jar tools/ )

target/tools/dsdgen: target/tpcds_kit.zip
	test -d target/tools/ || (cd target; unzip tpcds_kit.zip)
	test -d target/tools/ || (cd target; mv */tools tools)
	cd target/tools; cat ../../patches/all/*.patch | patch -p0
	cd target/tools; cat ../../patches/${MYOS}/*.patch | patch -p1
	cd target/tools; make clean; make dsdgen

clean:
	mvn clean