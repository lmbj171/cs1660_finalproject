Video walkthrough:
https://www.youtube.com/watch?v=NcaZ7-sCizI

Project Criteria:
1. First java application implementation and execution on Docker
2. Docker to local (or GCP) cluster communication 
3. Inverted indexing MapReduce implementation and execution on the cluster (GCP)

Extra Credit:
I did not implement term and top-N search, custom functionality, or a secondary sort algorithm;
however, I did implement the ability to choose files from the computer and upload them to the 
google cluster to have inverted indicies run on.  

I used the spotfiy-dataproc-java-submitter to submit jobs to the GCP cluster.

Environment Variables:
set-variable -name GOOGLE_APPLICATION_CREDENTIALS -value [PATH]
set-variable -name DISPLAY -value [IP]:0.0

Docker commands:
docker pull linnbjanes/finalproject:latest
docker run -it --privileged -e DISPLAY=$DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix linnbjanes/finalproject:latest
