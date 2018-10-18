# Apache Beam Example using FlinkRunner

## Code
First of all, we need to compile the code. You can use the **java** docker image for that purpose:
* docker run -ti -v $(pwd)/pipeline/:/src maven:3.5.3-jdk-8 bash

once in the container:

* cd /src
* mvn clean package -Pflink-runner

Now, the **pipeline/target** folder will contain a **jar** file with the ar.uba.fi.beam.WordCount in a lean and bundled version. The bundled version will be required within the Runners in order to have all the java dependencies in place to instanciate the job.

## Flink runner
We'll need to start a Flink server including Job and Task managers. The Job manager will provide a Flink Dashboard which will accept the **jar** bundle to launch the Job. The Task managers are slaves which allows horizontal scalability for job running.
* cd docker
* docker-compose up

or:

* docker-compose up --scale taskmanager=3

or:

* docker-compose up
* docker-compose scale taskmanager=3

Then, we can upload the Job code:
* Go to http://localhost:8081
* Click on 'Submit new Job'
* Click on 'Add New'
* Choose the file **pipeline/target/beam-java-example-bundled-1.0.jar**
* Click on 'Upload'
The **bundled jar** version is key for the execution. Maven compiles a bundled file when using the profile flag **-Pflink-runner**.

And, lauch it:
* Mark the checkbox next to **beam-java-example-bundled-1.0.jar** in the uploaded Jars' table.
* Define the entry class:
  * ar.uba.fi.distribuidos.WordCountPipeline
* Define the program arguments:
  * --runner=FlinkRunner
  
or:

  * --runner=FlinkRunner --wordsQty=1000 --windowSize=60 --output=/tmp/out.txt
* Click on 'Submit'

To check the results, you can jump into the job manager's containers and look for the output files:
* docker exec -ti docker_jobmanager_1 bash
Once in the container:
* cd /tmp
* cat output.txt-(time-window)-pane-*
 
i.e.:

* cat output.txt-2018-10-16T00\:11\:00.000Z-2018-10-16T00\:11\:10.000Z-pane-*
will print the output for the 10 secs window starting at 2018-10-16-11:00

