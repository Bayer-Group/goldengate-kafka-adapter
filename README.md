# What is this?
As our organization moves to a coherent microservice, event-driven architecture we still need to "keep the lights on".  We had a need to expose change streams that go into our Oracle monolith to other teams within the organization.  As a licensed Oracle GoldenGate user, we felt [GoldenGate](http://www.oracle.com/technetwork/middleware/goldengate/overview/index.html) -> [Apache Kafka](http://kafka.apache.org) allowed us to bridge that gap, allowing teams to keep the lights on in their respective applications while still transforming data into new SQL/NoSQL database technologies that can be optimized for various use cases.  While there are patterns that can be followed to bridge change streams exposed via GoldenGate into Kafka (e.g. Flume), we felt it was best to keep this component as simple as possible while still achieving the desired functionality.  This is our first swing at this and we fully expect this tool to grow and mature.  Please feel free to reach out to the contributors with any questions or comments.  As always, pull requests are welcomed and encouraged.

## Build and Deploy
### Building
We currently do not distribute prebuilt binaries for the adapter, as Oracle requires that you sign their licensing agreement in order to access the relevant GoldenGate jars.  While this is a bump in the distribution process, we've tried to make the process to build and deploy the adapter as painless as possible.  Currently, you should only need to run a few simple maven goals to have a JAR built and ready to roll.

After cloning the project, create a ./lib directory within the project and place the following GoldenGate jars in this new directory:  ggdbutil-<version>, gguserexitapi-<version>, ggutil-<version>.

Next, build the assembly with Maven:
```mvn clean assembly:single```

To date, we've verified that the adapter works with the 12.1.2.1.2.X series of GoldenGate JARs.  It may work with others in the 12.1.2 series as well, but we have not done the verification. 

The adapter needs to be built with a version of Java that is compatible with the version running on the GoldenGate server!

## Going forward...
In the future, we have plans to allow the adapter to integrate with the [Confluent](http://confluent.io) stream data platform 
