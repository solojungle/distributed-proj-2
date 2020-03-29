Michelle Wu
Ali Awari
28 March 2020

Lessons learned from building a distributed file system

Lession 1: Sending information using protobuffers

    In order to create a distributed file system similar to Apache Hadoop, we needed
to learn how to properly communicate between different servers to a main client, and
provide sufficient information to sustain a distributed enviroment.

Lession 2: Understanding how a distributed file system functions

    Using the Apache Hadoop documentation as well as documentation provided in the form
of a project description we spent time understanding how the entire file system works. NameNodes
provide persistent memory storage of filenames, as well as, the datablock names, and when the Client
requests it, the NameNode will also provide available DataNodes which contain these datablocks.
