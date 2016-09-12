
# Accumulo batch scanner performance test

This repo contains a simple test that compares the Accumulo scanner to the batch scanner.  The test
relies on [YCSB] to generate data.

### Generating data

The following steps show how to generate data for the test in this repository using [YCSB].

Run the following commands in the accumulo shell.

```
createtable usertable
addsplits -t usertable user1~ user2~ user3~ user4~ user5~ user6~ user7~ user8~ user9~
```

Download [ycsb-accumulo-binding-0.10.0.tar.gz] from the [0.1.0 release page][release010] and untar
it somewhere.  Change to that directory, edit `workloads/workloada` and change the recordcount line
to `recordcount=1000000`.

```sh
./bin/ycsb load accumulo -s -P workloads/workloada  -p accumulo.zooKeepers=localhost -p accumulo.columnFamily=ycsb -p accumulo.instanceName=uno -p accumulo.username=root -p accumulo.password=secret
```

After this load, compact the table in the Accumulo shell.

### Running the code

The following command will run the performance test in this repo.  

```bash
mvn -q clean compile exec:java
```

To switch between batch scanner and scanner, modify a commented out section of the code.  To change
the accumulo instance, modify `main()`.


[YCSB]: https://github.com/brianfrankcooper/YCSB
[release010]: https://github.com/brianfrankcooper/YCSB/releases/tag/0.10.0
[ycsb-accumulo-binding-0.10.0.tar.gz]: https://github.com/brianfrankcooper/YCSB/releases/download/0.10.0/ycsb-accumulo-binding-0.10.0.tar.gz

