<<<<<<< HEAD

# RealTime-Streaming-Pipeline
=======
## Project Description
In our project, we created a data processing system using a Kafka server, organized with Docker and Kubernetes. It has five main parts: Request, Producer, Consumer, Application, and Merge. Each part sends its results to the next one. In the Application and Merge parts, we use the Pascal-G algorithm on a Twitter dataset to find out how many clusters there are.
This project is part of a bigger effort to understand clusters in data that comes in quickly. We use Kafka and Kubernetes to manage and process this data, turning it into a graph for detailed study. Everything, including Kafka parts and the cluster analysis tool, is neatly organized in Kubernetes using Pods. The "PASCAL-G" algorithm, great for analyzing streaming data, is a key part of our analysis.
We've provided a figure below and more explanations later on for those who want to understand our system better. This simplified overview is meant for our GitHub page to help people get the gist of what our project does.

## Achievements
1. We developed a high-performance pipeline for analyzing streaming data graphs, using a containerized Kafka server in Kubernetes. The process involves data streams being processed by Producers and Consumers in Kafka, forming a weighted word co-occurrence graph. This graph is then analyzed by our "PASCAL-G" algorithm. The whole system, including all Kafka components and the algorithm, is organized in Kubernetes with several Pods. More details and a visual summary of the pipeline are provided in the following sections.
2. We upgraded our "PASCAL-G" algorithm, now using MPI for parallel processing. This algorithm clusters streaming data by creating online clusters with vectors called fingerprints, updated as new nodes join. It works in two steps: first, finding new clusters using hashing, and second, merging similar fingerprints. This makes it faster and more efficient, especially in parallel computing where different parts of the graph are processed simultaneously. The final clustering solution is obtained by combining all these parts.

## Design 
We've provided a figure below and more explanations later on for those who want to understand our system better. This simplified overview is meant for our GitHub page to help people get the gist of what our project does.


### Request Pods
To mimic data streaming in a more stable environment, we created an API simulator to input data into our system. This simulator has an API server that works in the background and can reach the raw data stored in a Pod's local storage area. A Request Client gets data from the API server and keeps it in a common storage area, which the Producer Pods also use. After this, the request pod waits until it gets a signal from the merge pod, the final step in our system. This signal tells the request pod to send the next set of data.

### Producer Pods
Each Producer Pod, which runs in a computing node, has a Producer Container, which can start up a number of Producer processes. This is how parallelism takes place at the Producer stage of the pipeline. Producer processes are tasked with retrieving the data from the source, in this case the API simulator, cleaning it with standard text preprocessing techniques (lemmatization, removing stop words, etc.) and accommodating it into Kafka brokers at the next stage. The data received by the Request Pod is systematically indexed, ensuring that each Producer Pod can accurately identify it and access its corresponding data. Producers process their data and read a bach of tweets to form an array consisting of two elements: the first element is the hash value of each word in a tweet, obtained by applying a hash function that converts each word into a number, and the second element is a list of all words that co-occur with that word in the same tweet. 

### Kafka Pods
The arrays created by the Producer Pods are then passed into a stack of Kafka brokers. The topics within the brokers are designated by specific numbers, such as 10, 100, or 1000. Each array is assigned to a particular topic based on the remainder obtained from dividing the first element of the array, which is the hash value, by the total number of topics. For example, if the hash value of a hashtag is 2040 and there are 1000 topics, the array containing this hash value as its first element, along with the corresponding co-occurring words array as the second element, would be assigned to topic number 40. This assignment is determined because the remainder of 2040 divided by 1000 is 40. In this setup, each Kafka broker operates within its own pod, meticulously deployed using the Bitnami Helm chart. Following a similar deployment strategy, every Zookeeper instance is also encapsulated in a dedicated pod, mirroring the individualized approach taken for the Kafka brokers. Also, for containerization, we employed Docker containers to ensure efficient and isolated environments. The parallelism parameter in Kafka is determined by the number of topic partitions. By adjusting this variable, we can effectively control the level of parallelism within the Kafka system. The hashing mechanism is explained in details below. 

### Consumer Pods
Each Consumer-App Pod in our system has two parts: a Consumer Container and an Application Container. Both share a storage space for better data handling within the pod. The Consumer Containers can run several Consumer processes at once. They connect to different topics in the Kafka brokers, with each consumer handling a specific set of topics. They process data batches made by Producers and put into Kafka brokers. The consumers turn these batches into CSR (Compressed Sparse Row) matrices, where the first part of each batch is a row and the second part (the array of co-occurring words) makes the columns. Every time a word pair occurs, we add one to their spot in the matrix. These CSR matrices are then stored in the shared space with the Application Containers, each file being part of the whole matrix.
The Application Container waits until it has all parts of the CSR matrix. It regularly checks the shared space for new data. If it finds new data, it waits for more. If there's no new data for a while, it starts processing. It combines all the CSR matrix parts into a big CSR matrix. Then, the "find-clusters" process inside this container does the first clustering on this big matrix. The results are saved to the shared storage, which the merge pod can also access.

### Merge Pod
The merge pod consists of a single merge container and includes a local persistent volume dedicated to storing the final clustering results. Additionally, this merge pod is configured to share the persistent volume with the application containers within the consumer-App pod, facilitating data access. The merge-clusters process inside of the merge container reads from the shared persistent volume, computes the final clustering step and writes the final result to its locally mounted persistent volume. The merge pod also waits until it is certain that it has received all initial clustering results, employing the same data reception mechanism as the application containers, as previously described. Once the clustering results are complete, the merge pod then sends a notification to the request pod. This notification signals the request pod to send the next batch of data to the producer pods.

### Hashing Technique
In our system's first three stages, we create a weighted word co-occurrence graph. This graph shows words as nodes, and edges link nodes when their corresponding words appear together in a document, like tweets on social media. We assign weights to these edges based on the frequency of word pairs occurring together within a certain timeframe, such as an hour or a day. A higher frequency results in a stronger edge weight, crucial for the graph clustering algorithm's effectiveness.
For graph representation, we use sparse adjacency vectors instead of traditional edge lists or adjacency matrices. This approach is key to handling streaming data, as it allows for independent node processing and parallel clustering tasks. Each vector represents a node and includes a weight for each co-occurrence with another node. To efficiently construct these vectors, we implement a hashing mechanism. Words are hashed into numeric indices for the vectors, managing potential collisions by using a broad range of hash values. This ensures minimal overlap of different words into the same hash value, keeping the system efficient and accurate.

## Data Set
Twitter/X, initially known as Twitter, is a widely-used social network and a key platform for online communication and information spread. It has been a focal point for studying information dissemination during political crises, as highlighted in several academic works. For our project, Twitter/X serves as a primary data source.
We sourced data from two repositories. First is the Github Russo Ukrainian War Dataset, providing tweet IDs. Using the Twitter API, we retrieved 57,384,192 tweets from 7,744,714 users, spanning from February 24, 2022, to February 14, 2023. The second repository is the COVID-19 Tweets repository, noted in a study by Chen E, Lerman K, and Ferrara E. This collection focuses on COVID-19 related tweets, totaling 1,785,043,839 English tweets gathered from January 2020 to February 2023, also retrieved using the Twitter API. These extensive datasets form the foundation of our analysis.

## Prerequisites
- Python 3.8.x
- Kafka-python 2.0.2
- Kubernetes  v1.28.3
- Docker 0.19.0
  
All the other required packages would be installed by developing the docker images. The list of these packages for producer, consumer, application, and merge pods are in requirments.txt file. For request pod, the list of packages are in request-requirments.txt file.


## Running Pipeline
For running the whole pipeline, you need to run the runPipeline.sh file in shell-scripts directory by this command:
#### ./shell-scripts/runPipeline.sh

runPipeline.sh included these stages:

##### Creating Docker Images 
- ./shell-scripts/docker-all-pods.sh

##### Deploying Kafka-Python 
We deploy Kafka Bitnami Helm Chart for installing Kafak-Python. In the latest published version, Kafka does not need Zookeepr. 
- ./shell-scripts/docker-all-pods.sh
- ./shell-scripts/helm-install.sh
  
##### Deploying Pods 
- ./shell-scripts/deploy-sts.sh
  
##### Running Pods  
- ./shell-scripts/run-all-pods-scripts.sh

### Results

The final result would be in the last pod, Merge Pod in path ./request-data.

### Contributaions
Dr. Patrick Bridges, University of New Mexico
Dr. Trilce Estrada, University of New Mexico
Soheila Jafari Khouzani, University of New Mexico
Nidia Vaquera Chavez, University of New Mexico

=======
# DataStreaming-Kafka



## Getting started

To make it easy for you to get started with GitLab, here's a list of recommended next steps.

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin https://gitlab.nrp-nautilus.io/sjafari/datastreaming-kafka.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](https://gitlab.nrp-nautilus.io/sjafari/datastreaming-kafka/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Automatically merge when pipeline succeeds](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing(SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thank you to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README
Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
>>>>>>> 6e08db8523c21127b46e27be15784825256ca427
