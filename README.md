## Course Project

For this project, my aim was to develop a data pipeline capable of managing the monthly extraction of rated chess games, performing standard transformations, and uploading the data to a data warehouse. The selected raw data is sourced from Lichess, the largest non-profit chess platform online, in the pgn.zst format. 

On a monthly basis, Lichess condenses the data of 90-100 million chess games, providing information about their rated matches and uploading it onto an open database. However, the data we receive comes in an uncommon format, necessitating additional preprocessing. To tackle this, I've developed a self-made module called ***pgn2csv*** to handle the task. While it may not be groundbreaking, it's a reliable solution that gets the job done.

You can find further details about the Lichess Database [here](https://database.lichess.org/).

## Data Pipeline 

I've made a deliberate choice to segregate the computing tasks (on a local machine) from the storage tasks (on the cloud) to gain insights and experience in interacting with both local and cloud environments, as well as understanding the interaction between Docker containers. However, this decision comes with two significant constraints for this project: network conditions and disk usage.

Therefore, for demonstration purposes, I will only run a mock pipeline that processes 1.2GB of raw data. Executing the full pipeline for 90 million games (which amounts to about 30GB of raw data) would require over 300GB of storage and more than 20 hours, depending on your network speed.

### Tech Stack
* Development Environment: WSL2
* Version Control: Github
* Containerization: Docker
* Infrastructure as Code (IaC): Terraform
* Storage: Cloud Storage
* Workflow Orchestration: Apache Airflow
* Data Transformation: Apache Spark
* Data Warehouse: Google BigQuery
* Data Visualization: Google Looker Studio

### High-level Design
![image](https://github.com/EdenHimmelB/Lichess-Datawarehouse-Batch-Pipeline/blob/47463a2fa7b4c6a224ba9b862ada715acde911b0/images/pipeline-flow.png?raw=true)
![image](https://github.com/EdenHimmelB/Lichess-Datawarehouse-Batch-Pipeline/blob/47463a2fa7b4c6a224ba9b862ada715acde911b0/images/airflow-flow.png?raw=true)

## Dashboard


## Reproduce 
### 1. Fork and Clone the Repository

### 2. Create a Google Cloud Platform Account (a free trial is available) on [Google Cloud Platform](https://cloud.google.com/).
#### Note your Cloud Info
After creating and claiming your free trial version of Google Cloud, note your working Project ID and modify it in the **.env** and **terraform/variables.tf** files accordingly.

#### Download Google Cloud SDK
Follow the official documentation [here](https://cloud.google.com/sdk/docs/install).

#### Set Up Credentials to Interact with Google Cloud Infra
Type this on your CLI and follow the instructions.
```
gcloud auth application-default login
```

### 3. Download and Setup Cloud Infrastructures with Terraform
* Follow the instructions at [Terraform page](https://developer.hashicorp.com/terraform/install) to download.
* Initialize Terraform using the CLI.
```
cd terraform/
terraform init
```
* Apply configs and activate all the necessary cloud infrastructures and APIs for this project.
```
terraform apply
```

***IMPORTANT***: Applying Terraform will create a service account and download the JSON key specifically for the project to your local machine at file path **keys/google_application_credentials.json**. This key will be used by Airflow and Spark for the following steps, so keep it secured.

If you are not familiar with Terraform and do not want to work with it or download your key locally, please skip and configure these assets manually in the GCP UI and save the service account key to your secured environment of choice.

### 4. Build Your Services Containers
Using your CLI, paste in these commands to spin up your Apache Airflow & Spark services.
```
cd ..
docker compose build
docker compose up -d
```

As simple as that, Docker is indeed a powerful and revolutionary tool.


### 5. Run Your Pipeline
* Access Airflow UI at [http://localhost:8080/home](http://localhost:8080/home)
* You would see that there are 2 DAGs: a full pipeline and a mock pipeline, I suggest running the latter one. It would be finished in more or less than 20 minutes, ~2.6M games.
![images](https://github.com/EdenHimmelB/Lichess-Datawarehouse-Batch-Pipeline/blob/47463a2fa7b4c6a224ba9b862ada715acde911b0/images/dags.png?raw=true)

### 6. Build Your Dashboard
![images](https://github.com/EdenHimmelB/Lichess-Datawarehouse-Batch-Pipeline/blob/47463a2fa7b4c6a224ba9b862ada715acde911b0/images/chart.png?raw=true)

### 7. Clean Up Cloud Infrastructures
```
cd terraform/
terraform destroy
```

This command will clean up all cloud infrastructures and their artifacts, including the **keys/google_application_credentials.json** file mentioned above.

## What's Next...
While separating compute and storage has provided valuable educational insights into the intricacies of connections between local environments and the cloud, as well as interactions between Docker containers, it has proven impractical in my situation due to poor network conditions and limited local resources. In real-life applications, it would be more advantageous to primarily build this project on cloud infrastructure. Specifically:

* Deploying a compute engine instance in the cloud to manage preprocessing tasks.
* Utilizing GCSFuse to integrate the directory containing data on that instance with a Cloud Storage bucket, thereby enhancing redundancy and eliminating an additional uploading task.
* Establishing an ephemeral Dataproc cluster to efficiently handle Spark job submissions.
* Implementing improved logging mechanisms across various steps to enhance monitoring and troubleshooting capabilities.